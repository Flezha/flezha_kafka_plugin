defmodule FlezhaKafka.Test.Helpers do
  @moduledoc """
  What the package's tests need from a running controller and nothing more: a
  process that waits, a poll, a telemetry capture, Mimic's global mode for one
  test, and the outbox owner a delivery-guaranteed output answers to.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Flezha.State.Outbox
  alias Flezha.State.Supervisor, as: StateSupervisor

  @doc "Block until told `:unpark`: a pid that stays alive and does nothing."
  def park, do: receive(do: (:unpark -> :ok))

  @doc "Poll `fun` every `interval_ms` until it is truthy or `timeout_ms` passes."
  def wait_until(fun, timeout_ms, interval_ms \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll(fun, deadline, interval_ms)
  end

  defp poll(fun, deadline, interval_ms) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) >= deadline do
          falsy
        else
          Process.sleep(interval_ms)
          poll(fun, deadline, interval_ms)
        end

      truthy ->
        truthy
    end
  end

  @doc """
  Forward every emission of `events` to the calling process as
  `{:telemetry, event, measurements, metadata}` until the test exits.
  """
  def capture(events) do
    events = if is_list(hd(events)), do: events, else: [events]
    test_pid = self()
    handler_id = {__MODULE__, events, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  @doc "Stop `pid` if it is still alive; a teardown has nothing to assert."
  def ensure_stopped(pid, reason \\ :normal, timeout \\ :infinity) do
    if is_pid(pid) and Process.alive?(pid) do
      try do
        GenServer.stop(pid, reason, timeout)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc """
  Prepare `opts` as the extension table does (the host's identity merged, the
  declaration's normalisation, `init_provider/1`), then start the declared
  workers through the table's own `Workers.start/5`. Answers the prepared opts
  and the plugin's worker.
  """
  def prepare_and_activate(provider, opts) do
    host = %{
      overwatcher_pid: self(),
      source_namespace: :default,
      source_operator_id: "test-operator",
      source_plugin_id: Map.get(opts, :plugin_id),
      subscribed_extensions: [:on_output]
    }

    plugin_id = Map.get(opts, :plugin_id) || "provider-prepare"

    with {:ok, normalised} <- Options.normalise_opts(provider, Map.merge(host, opts)),
         {:ok, prepared} <- provider.init_provider(normalised),
         {started, :ok} <-
           Workers.start(%{entries: %{}, monitors: %{}}, plugin_id, provider, prepared, & &1.()) do
      {:ok, prepared, Entry.owned_pid(started.entries[plugin_id])}
    else
      {_table, {:error, _} = err} -> err
      {:error, _} = err -> err
    end
  end

  @doc """
  The host operator of a feeding worker, linked to the caller: it answers each
  confirming ingest with the next of `verdicts`, `:ok` once they run out, and
  tells the caller `{:ingest, tag, body, metadata}`.
  """
  def stand_in_host(tag, verdicts \\ []) do
    test = self()
    spawn_link(fn -> host_loop(test, tag, verdicts) end)
  end

  defp host_loop(test, tag, verdicts) do
    receive do
      {:"$gen_call", from, {:ingest, body, metadata}} ->
        send(test, {:ingest, tag, body, metadata})
        {verdict, rest} = if verdicts == [], do: {:ok, []}, else: {hd(verdicts), tl(verdicts)}
        GenServer.reply(from, verdict)
        host_loop(test, tag, rest)
    end
  end

  @doc "Mimic's global mode for this test, released when it exits."
  def set_mimic_global_until_exit(_context \\ %{}) do
    Mimic.set_mimic_global()
    on_exit(fn -> Mimic.set_mimic_private() end)
    :ok
  end

  @doc "Register the calling test as the outbox owner of `cell_id`."
  def stub_outbox_owner(cell_id) do
    case Registry.start_link(keys: :unique, name: StateSupervisor.registry_name()) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    {:ok, _} = Registry.register(StateSupervisor.registry_name(), cell_id, nil)
    :ok
  end

  @doc """
  Metadata a durable outbox flush would stamp on one output of `cell_id`'s
  entry `seq`: identity plus the confirmation token the owning writer waits on.
  """
  def guaranteed_metadata({rc, sh, pk} = cell_id, seq, extra \\ %{}) do
    Map.merge(
      %{
        "channel" => "out",
        "flz.cell" => "#{rc}/#{sh}/#{pk}",
        "flz.seq" => "#{seq}.0",
        "flz.epoch" => "0",
        Outbox.ack_key() => Outbox.token(cell_id, {:cursor, seq})
      },
      extra
    )
  end

  @doc "Wait until every one of `keys` has been confirmed to the calling owner."
  def assert_confirmed(keys, timeout \\ 2_000), do: await_confirmed(MapSet.new(keys), timeout)

  defp await_confirmed(remaining, timeout) do
    if MapSet.size(remaining) == 0 do
      :ok
    else
      receive do
        {:"$gen_cast", {:outbox_confirmed_many, keys}} ->
          await_confirmed(Enum.reduce(keys, remaining, &MapSet.delete(&2, &1)), timeout)

        {:"$gen_cast", {:outbox_confirmed, key}} ->
          await_confirmed(MapSet.delete(remaining, key), timeout)
      after
        timeout ->
          ExUnit.Assertions.flunk("never confirmed: #{inspect(MapSet.to_list(remaining))}")
      end
    end
  end
end
