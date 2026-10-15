defmodule FlezhaKafka.ProducerWorker.Lanes do
  @moduledoc """
  The producer worker's partition lanes. Each partition the worker produces to
  has one: its bounded queue, linger timer, in-flight window, retry timer,
  waiting callers and idempotent producer session, which is what one worker per
  partition held before a worker served a whole topic.

  One lane at a time is held in the worker's state map, under the very keys the
  per-partition code (`FlezhaKafka.ProducerWorker.Window`) reads, and the others
  are parked in `lanes`. `hold/2` swaps the held one, so a worker producing to a
  single partition never swaps at all. Lane `nil` holds the records that arrive
  before the topic's partition count is known; it never sends.
  """

  alias Flezha.Process.Backlog
  alias FlezhaKafka.ProducerWorker.Window

  @fields [
    :partition,
    :producer_id,
    :producer_epoch,
    :sequence,
    :idempotent,
    :max_in_flight,
    :queue,
    :linger_timer,
    :in_flight,
    :retry_timer,
    :blocked,
    :consecutive_failures,
    :last_error
  ]

  @doc """
  A fresh lane for `partition`, its session as configured and not yet started,
  its queue bounded by `queue_max` records alone.
  """
  @spec new(non_neg_integer() | nil, boolean(), non_neg_integer(), pos_integer()) :: map()
  def new(partition, idempotent, max_in_flight, queue_max) do
    %{
      partition: partition,
      producer_id: nil,
      producer_epoch: nil,
      sequence: 0,
      # The waiting lane is never sent from: no window slot ever frees.
      idempotent: idempotent and partition != nil,
      max_in_flight: if(partition == nil, do: 0, else: max_in_flight),
      queue: Backlog.new(:infinity, queue_max),
      linger_timer: nil,
      # Batches outstanding on the wire, oldest first.
      in_flight: [],
      retry_timer: nil,
      blocked: :queue.new(),
      # Batches given up on in a row, and the last reason, are a partition's
      # own: a partition that delivers says nothing about one that does not.
      consecutive_failures: 0,
      last_error: nil
    }
  end

  @doc """
  Hold lane `partition`, parking the one held before. Answers whether the lane
  was created by this call, so the worker can start its producer session.
  """
  @spec hold(map(), non_neg_integer() | nil) :: {:held | :created, map()}
  def hold(%{partition: partition} = state, partition), do: {:held, state}

  def hold(state, partition) do
    parked = Map.put(state.lanes, state.partition, Map.take(state, @fields))

    case Map.pop(parked, partition) do
      {nil, lanes} -> {:created, Map.merge(%{state | lanes: lanes}, fresh(state, partition))}
      {lane, lanes} -> {:held, Map.merge(%{state | lanes: lanes}, lane)}
    end
  end

  defp fresh(state, partition),
    do:
      new(
        partition,
        state.configured_idempotent,
        state.configured_max_in_flight,
        state.queue_max
      )

  @doc "Remove lane `partition`, holding the waiting lane if it was the held one."
  @spec drop(map(), non_neg_integer() | nil) :: map()
  def drop(state, nil), do: state

  def drop(state, partition) do
    {_, state} = hold(state, nil)
    %{state | lanes: Map.delete(state.lanes, partition)}
  end

  @doc """
  Apply `fun` to every lane in partition order, then hold again the lane that
  was held before, so a caller in the middle of one lane's work continues on it.
  """
  @spec each(map(), (map() -> map())) :: map()
  def each(state, fun) do
    held = state.partition

    state
    |> partitions()
    |> Enum.reduce(state, fn partition, acc ->
      {_, acc} = hold(acc, partition)
      fun.(acc)
    end)
    |> hold(held)
    |> elem(1)
  end

  @doc "The partition of the lane `match?` accepts, held or parked."
  @spec find(map(), (map() -> boolean())) :: {:ok, non_neg_integer() | nil} | :error
  def find(state, match?) do
    case Enum.find(all(state), fn {_partition, lane} -> match?.(lane) end) do
      {partition, _lane} -> {:ok, partition}
      nil -> :error
    end
  end

  @doc "Every lane, the held one included, by partition."
  @spec all(map()) :: %{(non_neg_integer() | nil) => map()}
  def all(state), do: Map.put(state.lanes, state.partition, Map.take(state, @fields))

  @doc "Every lane's partition, the waiting lane first."
  @spec partitions(map()) :: [non_neg_integer() | nil]
  def partitions(state), do: state |> all() |> Map.keys() |> Enum.sort_by(&(&1 || -1))

  @doc """
  Mark the held lane full, or not. The worker's full flag, which a dispatch
  reads before it casts, is up while any lane is marked.
  """
  @spec mark_full(map(), boolean()) :: map()
  def mark_full(state, full?) do
    full =
      if full?,
        do: MapSet.put(state.full, state.partition),
        else: MapSet.delete(state.full, state.partition)

    if MapSet.size(full) == 0 != (MapSet.size(state.full) == 0),
      do: :atomics.put(state.flag, 1, min(MapSet.size(full), 1))

    %{state | full: full}
  end

  @doc """
  Take out every record the held lane holds, its window's first, then its
  queue, then those waiting for room, whose dispatches are let go. The lane is
  left empty, its timers cancelled.
  """
  @spec empty(map()) :: {[map()], map()}
  def empty(state) do
    state = Window.cancel_timers(state)

    waiting =
      for {from, record} <- :queue.to_list(state.blocked) do
        Window.release(from)
        record
      end

    held = Enum.flat_map(state.in_flight, & &1.records) ++ Backlog.to_list(state.queue) ++ waiting
    state = %{state | queue: Backlog.clear(state.queue), in_flight: [], blocked: :queue.new()}
    {held, mark_full(state, false)}
  end

  @doc "The lane that has given up on the most batches in a row, the worker's health."
  @spec worst(map()) :: map()
  def worst(state), do: state |> all() |> Map.values() |> Enum.max_by(& &1.consecutive_failures)

  @doc "Records queued, in flight and waiting for room, summed over every lane."
  @spec totals(map()) :: %{
          queue_depth: non_neg_integer(),
          in_flight: non_neg_integer(),
          waiting: non_neg_integer(),
          queue_bytes: non_neg_integer()
        }
  def totals(state) do
    zero = %{queue_depth: 0, in_flight: 0, waiting: 0, queue_bytes: 0}

    Enum.reduce(all(state), zero, fn {_p, lane}, acc ->
      %{
        queue_depth: acc.queue_depth + lane.queue.frames,
        in_flight: acc.in_flight + window_records(lane),
        waiting: acc.waiting + :queue.len(lane.blocked),
        queue_bytes: acc.queue_bytes + lane.queue.bytes
      }
    end)
  end

  @doc "Records across every batch a lane has in flight."
  @spec window_records(map()) :: non_neg_integer()
  def window_records(lane),
    do: Enum.reduce(lane.in_flight, 0, fn batch, n -> n + length(batch.records) end)
end
