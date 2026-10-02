defmodule FlezhaKafka.DocsTest do
  use ExUnit.Case, async: true

  alias Flezha.Plugin.ProviderBehaviour.Options

  @page Path.join(Mix.Project.deps_paths()[:flezha], "docs/plugins/kafka_producer.md")

  test "the core's page carries the option table the provider's declaration renders" do
    rendered = Options.render_options_table(FlezhaKafka.Producer)

    documented =
      @page
      |> File.read!()
      |> String.split("<!-- provider-options:begin -->\n")
      |> Enum.at(1, "")
      |> String.split("<!-- provider-options:end -->")
      |> hd()

    assert documented == rendered,
           "#{@page}'s option table has drifted from FlezhaKafka.Producer's declaration.\n" <>
             "Replace the block between the provider-options markers with:\n\n" <> rendered
  end
end
