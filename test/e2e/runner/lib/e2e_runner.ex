defmodule E2eRunner do
  @moduledoc """
  The relays the end-to-end run binds to the Kafka producer:

    * `echo` (flow, at least once): emits every input on the sink `out`.
    * `echo_once` (flow, effectively once): the same, each output held by the
      outbox until the producer confirms it, so a broker outage loses nothing.
  """

  defmodule Echo do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "echo", input: "in", sink: "out"

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule EchoOnceTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "echo_once_tasklet"

    @impl true
    def init_state(_key), do: %{}

    # A keyed sink refuses a plain `emit`; the input is its own key.
    @impl true
    def run(input, state, ctx) do
      :ok = emit_keyed(ctx, input, input)
      {:ok, state}
    end
  end

  # A plugin-bound channel of an effectively-once relay is keyed: its outputs
  # carry their key as `flz.key`.
  defmodule EchoOnce do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "echo_once", run: {:flow, exactly: :effectively_once}

    @impl true
    def channels(c),
      do: c |> input("in") |> into(E2eRunner.EchoOnceTask) |> keyed() |> sink("out")
  end
end
