# The producer's admission over its partition lanes, with no broker: records
# through `FlezhaKafka.ProducerWorker.Window.admit_record/3` over a worker state
# holding two lanes. Lane 0 takes fifteen records in sixteen, lane 1 the rest;
# a lane admitted past its bound is emptied, so each fills and drains, and the
# full flag goes up and down. Prints one line a script can read:
#
#     [bench] kafka admission n=<N> wall=<us>us per_record=<ns>ns
#
# Run from the package directory (the default is 1000000 records):
#
#     mix run --no-start bench/admission.exs [records]

defmodule FlezhaKafka.Bench.Admission do
  @moduledoc false

  alias Flezha.Process.Backlog
  alias FlezhaKafka.ProducerWorker.Lanes
  alias FlezhaKafka.ProducerWorker.Wire
  alias FlezhaKafka.ProducerWorker.Window

  @queue_max 1_000

  def run(records) do
    record = Wire.build_record("bench-record-0123456789abcdef", %{}, :async, 0, [])
    state = two_lanes()
    admit_all(state, record, 10_000)
    {wall_us, _state} = :timer.tc(fn -> admit_all(state, record, records) end)

    IO.puts(
      "[bench] kafka admission n=#{records} wall=#{wall_us}us " <>
        "per_record=#{div(wall_us * 1_000, max(records, 1))}ns"
    )
  end

  # The fields admission and the lane swap read, as the worker holds them.
  defp two_lanes do
    state =
      Map.merge(Lanes.new(0, false, 1, @queue_max), %{
        lanes: %{},
        configured_idempotent: false,
        configured_max_in_flight: 1,
        queue_max: @queue_max,
        queue_overflow: :drop_nothing,
        queue_block_timeout_ms: 5_000,
        flag: :atomics.new(1, []),
        full: MapSet.new(),
        dropped_total: 0
      })

    {:created, state} = Lanes.hold(state, 1)
    {:held, state} = Lanes.hold(state, 0)
    state
  end

  defp admit_all(state, record, records) do
    Enum.reduce(1..records//1, state, fn n, state ->
      {_held, state} = Lanes.hold(state, if(rem(n, 16) == 0, do: 1, else: 0))
      {:admitted, state} = Window.admit_record(state, record, :async)
      drain_past_bound(state)
    end)
  end

  defp drain_past_bound(%{queue: %Backlog{frames: frames}} = state) when frames > @queue_max,
    do: Lanes.mark_full(%{state | queue: Backlog.clear(state.queue)}, false)

  defp drain_past_bound(state), do: state
end

case System.argv() do
  [records | _] -> FlezhaKafka.Bench.Admission.run(String.to_integer(records))
  [] -> FlezhaKafka.Bench.Admission.run(1_000_000)
end
