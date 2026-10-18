defmodule FlezhaKafka.DocsTest do
  use ExUnit.Case, async: true

  alias Flezha.Plugin.ProviderBehaviour.Options
  alias FlezhaTest.Helper.SourceWalk

  @pages Path.join(Mix.Project.deps_paths()[:flezha], "docs/public/plugins")

  for {provider, page} <- [
        {FlezhaKafka.Producer, "kafka_producer.md"},
        {FlezhaKafka.Consumer, "kafka_consumer.md"}
      ] do
    test "the core's #{page} carries the option table #{inspect(provider)} renders" do
      provider = unquote(provider)
      page = Path.join(@pages, unquote(page))
      rendered = Options.render_options_table(provider)

      assert SourceWalk.block(page, "provider-options", provider.__provider__().name) == rendered,
             "#{page}'s option table has drifted from #{inspect(provider)}'s declaration.\n" <>
               "Replace the block between its provider-options markers with:\n\n" <> rendered
    end
  end
end
