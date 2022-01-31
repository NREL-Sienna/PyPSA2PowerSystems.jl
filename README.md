# PyPSA2PowerSystems.jl

Simple Julia module to translate PyPSA outputs into a [PowerSystems.jl](https://github.com/nrel-siip/PowerSystems.jl) `System`.

*There are several known limitations to this translation. The known limitations are documented in the [issues](https://github.com/NREL-SIIP/PyPSA2PowerSystems.jl/issues)*
## Instructions

```julia
using Pkg
Pkg.add("https://github.com/NREL-SIIP/PyPSA2PowerSystems.jl.git")

System("path/to/pypsa_output/")
```
