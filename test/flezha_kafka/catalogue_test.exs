defmodule FlezhaKafka.CatalogueTest do
  @moduledoc """
  The refusals the producer makes are codes of the core's one catalogue. Built
  against a core that lacks one, this suite fails, not a user's enable.
  """
  use ExUnit.Case, async: true

  @catalogue Mix.Project.deps_paths()[:flezha]
             |> Path.join("crates/flz_transport/errors.json")
             |> File.read!()
             |> Jason.decode!()

  test "the producer's tagged refusals map to their codes" do
    for {reason, code} <- [
          {{:headers_invalid, "kafka_producer", :headers, "flz.key"}, "plugin.headers_invalid"},
          {{:window_needs_idempotence, 5}, "plugin.window_needs_idempotence"}
        ] do
      assert Map.has_key?(@catalogue["codes"], code), "#{code} is not in the catalogue"
      assert %Flezha.Error{code: ^code} = Flezha.Error.from_reason(reason, nil)
    end
  end
end
