require Rustler
defmodule KeenBatch do
  @on_load :load_nif

  defp load_nif do
    :ok = Rustler.load_nif("keenbatch")
  end

  # When loading a NIF module, dummy clauses for all NIF function are required.
  # NIF dummies usually just error out when called when the NIF is not loaded, as that should never normally happen.
  def add(_arg1, _arg2), do: exit(:nif_not_loaded)
end