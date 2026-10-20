defmodule FlezhaKafka.ProducerWorkerBlockedBoundTest do
  @moduledoc """
  The chain dispatch in front of a full worker under `:drop_nothing`: the worker
  raises its full flag, and a dispatch that reads it up waits in a call until
  there is room, at most `queue_block_timeout_ms`, after which its record
  overflows. The worker holds `queue_max` records plus the one that found the
  queue full on its way, never a queue of everything dispatched since.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.WorkerSup
  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Fakes
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ProviderPrepare

  @queue_max 10
  @block_ms 100

  @tag :tmp_dir
  test "a dispatch to a full worker waits, then its record overflows after queue_block_timeout_ms",
       %{tmp_dir: dir} do
    original = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.put(original, :dir, dir))
    on_exit(fn -> Application.put_env(:flezha, :dlq, original) end)
    start_supervised!(DlqStore)
    rc = "kb689_" <> Integer.to_string(System.unique_integer([:positive]))
    cell_id = {rc, "task", "k1"}
    :ok = ProviderPrepare.stub_outbox_owner(cell_id)

    {pid, opts} = start_full_worker(queue_block_timeout_ms: @block_ms)
    Fakes.attach(:dropped)

    late = [ProviderPrepare.guaranteed_metadata(cell_id, 1) | List.duplicate(%{}, 19)]

    log =
      capture_log(fn ->
        tasks = for md <- late, do: Task.async(fn -> timed_dispatch(opts, md) end)

        assert NodeTestHelper.wait_until(
                 fn -> ProducerWorker.stats(pid)[:waiting] == 20 end,
                 1_000,
                 5
               )

        for elapsed_ms <- Task.await_many(tasks, 2_000), do: assert(elapsed_ms >= @block_ms)
        assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 1}], 1_000)
      end)

    stats = ProducerWorker.stats(pid)
    assert %{waiting: 0, queue_depth: 11, in_flight: 1, dropped_total: 20} = stats

    for _ <- 1..20 do
      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :overflowed], %{count: 1},
                      %{overflow_strategy: :drop_nothing}}
    end

    assert log =~ "parked undeliverable output"
    assert [entry] = DlqStore.list(rc)

    assert %{"code" => "kafka.exhausted", "details" => %{"reason" => "queue_full"}} =
             Jason.decode!(entry["error"])
  end

  test "a waiting dispatch is let in once a sent batch makes room" do
    {pid, opts} =
      start_full_worker(queue_block_timeout_ms: 60_000, send_fn: Fakes.forward_send_fn(self()))

    assert_received {:dispatched, first, _request}
    task = Task.async(fn -> dispatch(opts, "late") end)
    assert NodeTestHelper.wait_until(fn -> ProducerWorker.stats(pid)[:waiting] == 1 end, 1_000, 5)

    Fakes.answer(pid, first, Fakes.ok_response(Fakes.topic(), Fakes.partition()))

    assert {:ok, %Ctx.IO{message: "late"}} = Task.await(task, 5_000)
    assert_receive {:dispatched, _second, _request}, 1_000
    assert %{waiting: 0, queue_depth: 1, dropped_total: 0} = ProducerWorker.stats(pid)
  end

  # A registered worker whose first batch is in flight, never answered unless
  # the test does, and whose queue holds `queue_max` records and one more: the
  # one that found it full on its way, which raised the flag.
  defp start_full_worker(overrides) do
    key = {"default", "kb689-#{System.unique_integer([:positive])}", Fakes.topic()}

    {pid, _script} =
      Fakes.start_worker(
        [
          name: WorkerSup.via(WorkerSup.family(Producer), key),
          queue_max: @queue_max,
          queue_overflow: :drop_nothing,
          linger_ms: 1,
          delivery_timeout_ms: 600_000,
          extra: [dispatch_timeout_ms: 600_000]
        ] ++ overrides
      )

    opts = %{filter_channel: nil, producer: WorkerSup.worker(Producer, key)}
    dispatch(opts, "first")
    assert Fakes.in_flight?(pid, 1)
    for i <- 1..(@queue_max + 1), do: dispatch(opts, "fill-#{i}")
    # A call answers once every cast ahead of it was taken in.
    _ = ProducerWorker.stats(pid)
    {pid, opts}
  end

  defp dispatch(opts, message, metadata \\ %{}) do
    Producer.on_output(
      %Ctx.IO{relay_id: "host", namespace: :default, message: message, metadata: metadata},
      opts
    )
  end

  defp timed_dispatch(opts, metadata) do
    started = System.monotonic_time(:millisecond)
    {:ok, _ctx} = dispatch(opts, "late", metadata)
    System.monotonic_time(:millisecond) - started
  end
end
