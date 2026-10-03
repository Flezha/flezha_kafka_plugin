defmodule FlezhaKafka.DocsTest do
  use ExUnit.Case, async: true

  alias Flezha.Plugin.ProviderBehaviour.Options

  @pages Mix.Project.deps_paths()[:flezha] |> Path.join("docs/plugins")

  for {provider, page} <- [
        {FlezhaKafka.Producer, "kafka_producer.md"},
        {FlezhaKafka.Consumer, "kafka_consumer.md"}
      ] do
    test "the core's #{page} carries the option table #{inspect(provider)} renders" do
      page = Path.join(@pages, unquote(page))
      rendered = Options.render_options_table(unquote(provider))

      documented =
        page
        |> File.read!()
        |> String.split("<!-- provider-options:begin -->\n")
        |> Enum.at(1, "")
        |> String.split("<!-- provider-options:end -->")
        |> hd()

      assert documented == rendered,
             "#{page}'s option table has drifted from #{inspect(unquote(provider))}'s declaration.\n" <>
               "Replace the block between the provider-options markers with:\n\n" <> rendered
    end
  end
end
