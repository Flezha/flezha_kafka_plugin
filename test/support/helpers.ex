defmodule FlezhaKafka.Test.Helpers do
  @moduledoc """
  What the package's tests need beyond the core's shared helpers: a plugin
  prepared and activated as the extension table does it, and a stand-in relay.
  """

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.ProviderBehaviour.Options

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
      source_relay_id: "test-relay",
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
  The host relay of a feeding worker, linked to the caller: it answers each
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
end
