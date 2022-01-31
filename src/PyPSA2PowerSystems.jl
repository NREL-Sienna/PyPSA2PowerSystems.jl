module PyPSA2PowerSystems

import PowerSystems
using DataFrames
using CSV

export System
export format_pypsa

const OUT = "formatted"

function System(src_folder::AbstractString)

    format_pypsa(src_folder)
    # Make a PSY System
    rawsys = PowerSystems.PowerSystemTableData(
        joinpath(src_folder, OUT),
        100.0,
        joinpath(dirname(dirname(pathof(PyPSA2PowerSystems))), "deps", "user_descriptors.yaml"),
        #timeseries_metadata_file = joinpath(dirname(dirname(pathof(PyPSA2PowerSystems))), "deps", "timeseries_pointers.json"),
        generator_mapping_file = joinpath(dirname(dirname(pathof(PyPSA2PowerSystems))), "deps", "generator_mapping.yaml"),
    )
    return PowerSystems.System(rawsys;)# time_series_resolution = Dates.Hour(1))
end

function format_pypsa(src_folder::AbstractString)
    mkpath(joinpath(src_folder, OUT))
    format_bus(src_folder)
    format_branch(src_folder)
    format_gen(src_folder)
    format_load(src_folder)
end

function format_bus(src_folder::AbstractString)
    buses = CSV.read(joinpath(src_folder, "buses.csv"), DataFrame)
    buses.id = buses.name
    buses.name = "bus_" .* string.(buses.name)
    CSV.write(joinpath(src_folder, OUT, "bus.csv"), buses)
end

function format_branch(src_folder::AbstractString)
    lines = CSV.read(joinpath(src_folder, "lines.csv"), DataFrame)
    transformers = CSV.read(joinpath(src_folder, "transformers.csv"), DataFrame)

    lines.tap_position .= 0.0
    lines.is_transformer .= false
    lines.name = "line_" .* string.(lines.name)
    transformers.is_transformer .= true
    transformers.b .= 0.0
    transformers.name = "transformer_" .* string.(transformers.name)

    common_cols = intersect(names(lines), names(transformers))
    branches = vcat(lines[:,common_cols], transformers[:,common_cols])

    branches.bus0 = "bus_" .* string.(branches.bus0)
    branches.bus1 = "bus_" .* string.(branches.bus1)

    CSV.write(joinpath(src_folder, OUT, "branch.csv"), branches)
end

function format_gen(src_folder::AbstractString)
    gens = CSV.read(joinpath(src_folder, "generators.csv"), DataFrame)
    gens.name = "gen_" .* string.(gens.name)
    gens.bus = "bus_" .* string.(gens.bus)

    gens.zero .= 0.0

    if !hasproperty(gens, :carrier)
        gens.carrier .= "OCGT" #make a dummy carrier col
    end

    gens.fuel = replace.(gens.carrier, "OCGT" => "NG")
    gens.unit_type = replace(gens.carrier, "OCGT" => "GT")

    CSV.write(joinpath(src_folder, OUT, "gen.csv"), gens)
end

function format_load(src_folder::AbstractString)
    loads = CSV.read(joinpath(src_folder, "loads.csv"), DataFrame)
    loads.name = "load_" .* string.(loads.name)
    loads.bus = "bus_" .* string.(loads.bus)
    CSV.write(joinpath(src_folder, OUT, "load.csv"), loads)
end

end # module
