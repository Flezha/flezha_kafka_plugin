defmodule Relay.MixProject do
  use Mix.Project

  # The end-to-end fixture: an Elixir SDK runner whose operators emit every input
  # on the sink `out`, which the Kafka producer under test publishes. It is a test,
  # not an example: `test/e2e/run.sh` builds and starts it. The SDK is the one in
  # the core checkout beside this repository.
  def project do
    [
      app: :relay,
      version: "0.1.0",
      elixir: "~> 1.17",
      deps: [{:flezha_sdk, path: "../../../../../sdk/elixir"}]
    ]
  end

  def application do
    [mod: {Relay.Application, []}, extra_applications: [:logger]]
  end
end
