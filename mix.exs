defmodule FlezhaKafka.MixProject do
  use Mix.Project

  # The Kafka plugin package, packaged with `mix flezha.plugin.package` and loaded
  # by a Flezha controller at boot. `flezha` is the core this package is a
  # submodule of, two directories up, until it is published.
  def project do
    [
      app: :flezha_kafka,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: [test: "test --no-start"],
      # The end-to-end scripts are run by hand against a release.
      test_ignore_filters: [&String.starts_with?(&1, "test/e2e/")],
      package: [licenses: ["Apache-2.0"]]
    ]
  end

  # No `mod`: the providers' workers and the producer's broker-client registry
  # are started under each provider's family by the controller.
  def application do
    [
      env: [
        flezha_providers: %{
          "kafka_producer" => FlezhaKafka.Producer,
          "kafka_consumer" => FlezhaKafka.Consumer
        }
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:flezha, path: "../.."},
      # The owner's fork, which carries the producer's recovery fixes. Its
      # checksum library `crc32cer` is native code, which a package may not
      # carry: the controller release ships it.
      {:kafka_ex, github: "alfetahe/kafka_ex", ref: "0764363353dc2ffc9996bdc8af11b9fe00561a57"}
    ]
  end
end
