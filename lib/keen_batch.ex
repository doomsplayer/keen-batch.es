defmodule KeenBatch do
  require Rustler
  @on_load :load_nif

  def load_nif do
    :ok = Rustler.load_nif("keenbatch")
  end

  # When loading a NIF module, dummy clauses for all NIF function are required.
  # NIF dummies usually just error out when called when the NIF is not loaded, as that should never normally happen.
  def new_client(_arg1, _arg2), do: exit(:nif_not_loaded)
  def set_redis(_arg1, _arg2), do: exit(:nif_not_loaded)
  def set_timeout(_arg1, _arg2), do: exit(:nif_not_loaded)
  def new_query(_arg1, _arg2, _arg3, _arg4, _arg5, _arg6), do: exit(:nif_not_loaded)
  def group_by(_arg1, _arg2), do: exit(:nif_not_loaded)
  def filter(_arg1, _arg2), do: exit(:nif_not_loaded)
  def interval(_arg1, _arg2), do: exit(:nif_not_loaded)
  def other(_arg1, _arg2), do: exit(:nif_not_loaded)
  def accumulate(_arg1, _arg2), do: exit(:nif_not_loaded)
  def send_query(_arg1), do: exit(:nif_not_loaded)
  def range(_arg1, _arg2, _arg3), do: exit(:nif_not_loaded)
  def select(_arg1, _arg2, _arg3, _arg4), do: exit(:nif_not_loaded)
  def to_redis(_arg1, _arg2, _arg3), do: exit(:nif_not_loaded)
  def from_redis(_arg1, _arg2, _arg3), do: exit(:nif_not_loaded)
  def to_string(_arg1), do: exit(:nif_not_loaded)
end