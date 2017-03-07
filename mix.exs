defmodule KeenBatch.Mixfile do
  use Mix.Project

  def project do
    [app: :keen_batch,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     compilers: [:rustler] ++ Mix.compilers,
     rustler_crates: [
       keen_batch: [
         path: "./native/keenbatch",
         mode: (if Mix.env == :prod, do: :release, else: :debug),
       ]
     ],
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  def deps do
    [{:rustler, "~>0.8.0"}]
  end
end
