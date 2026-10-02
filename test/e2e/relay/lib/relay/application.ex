defmodule Relay.Application do
  @moduledoc """
  The runner: `mix run --no-halt -- <namespace>` serves the two factories in that
  namespace over the controller's Unix socket, and stops when its standard input
  closes.
  """
  use Application

  @impl true
  def start(_type, _args) do
    namespace = List.first(System.argv(), "default")
    watch_stdin()

    children = [
      {Flezha.SDK, namespace: namespace, operators: [Relay.Echo, Relay.EchoOnce], transport: :uds}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  defp watch_stdin do
    spawn(fn ->
      if device = Process.whereis(:user), do: Process.group_leader(self(), device)
      IO.read(:stdio, :eof)
      System.halt(0)
    end)
  end
end
