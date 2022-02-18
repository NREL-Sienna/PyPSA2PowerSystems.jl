module PyPSA2PowerSystems

import PowerSystems
import NetCDF
import JSON3
import DataFrames
import CSV
import Dates

export System
export format_pypsa

const BUS_PREFIX = "buses_"
const LOAD_PREFIX = "loads_"
const GEN_PREFIX = "generators_"
const LINE_PREFIX = "lines_"
const TFMR_PREFIX = "transformers_"

const PYPSA_PSY_CAT = Dict("Load" => "PowerLoad", "Generator" => "Generator")

const PYPSA_CATS = Dict(
    "offwind-ac" => (prime_mover = "WS", fuel = "WIND"),
    "onwind" => (prime_mover = "WT", fuel = "NG"),
    "solar" => (prime_mover = "PV", fuel = "SOLAR"),
    "CCGT" => (prime_mover = "GT", fuel = "NG"),
    "OCGT" => (prime_mover = "CT", fuel = "NG"),
    "ror" => (prime_mover = "HY", fuel = "WATER"),
    "biomass" => (prime_mover = "ST", fuel = "AG_BIPRODUCT"),
    "nuclear" => (prime_mover = "ST", fuel = "NUC"),
    "offwind-dc" => (prime_mover = "WS", fuel = "WIND"),
    "geothermal" => (prime_mover = "GT", fuel = "GEO"),
)

function System(src_file::AbstractString; cleanup = true)

    if isfile(src_file) && endswith(src_file, ".nc")
        out_path = format_pypsa(src_file; cleanup = cleanup)
    elseif isdir(src_file)
        out_path = src_file
    else
        throw(error("must be a valid PyPSA netcdf file or directory"))
    end
    # Make a PSY System
    rawsys = PowerSystems.PowerSystemTableData(
        out_path,
        100.0,
        joinpath(
            dirname(dirname(pathof(PyPSA2PowerSystems))),
            "deps",
            "user_descriptors.yaml",
        ),
        #timeseries_metadata_file = joinpath(dirname(dirname(pathof(PyPSA2PowerSystems))), "deps", "timeseries_pointers.json"),
        generator_mapping_file = joinpath(
            dirname(dirname(pathof(PyPSA2PowerSystems))),
            "deps",
            "generator_mapping.yaml",
        ),
    )
    return PowerSystems.System(rawsys;)# time_series_resolution = Dates.Hour(1))
end

function format_pypsa(src_file::AbstractString; cleanup = true)
    out_path = mktempdir(joinpath(dirname(src_file)), cleanup = cleanup)
    format_bus(src_file, out_path)
    format_branch(src_file, out_path)
    format_gen(src_file, out_path)
    format_load(src_file, out_path)
    format_timeseries(src_file, out_path)
    return out_path
end

function get_nc_var(data::NetCDF.NcFile, var::String, default = nothing)
    return haskey(data.vars, var) ? NetCDF.readvar(data[var]) : default
end

function format_bus(src_file::AbstractString, out_path::AbstractString)
    NetCDF.open(src_file) do data

        name = BUS_PREFIX .* get_nc_var(data, "buses_i")
        nbus = length(name)
        v_nom = get_nc_var(data, "buses_v_nom")
        v_mag_pu_set = get_nc_var(data, "buses_v_mag_pu_set", ones(nbus))
        control = get_nc_var(data, "buses_control", repeat(["PV"], nbus))
        buses = DataFrames.DataFrame(
            :id => 1:nbus,
            :name => name,
            :v_nom => v_nom,
            :v_mag_pu_set => v_mag_pu_set,
            :control => control,
        )
        CSV.write(joinpath(out_path, "bus.csv"), buses)
    end
end

function format_branch(src_file::AbstractString, out_path::AbstractString)
    NetCDF.open(src_file) do data
        branches = []
        for br in (LINE_PREFIX, TFMR_PREFIX)
            id = get_nc_var(data, "$(br)i")
            isnothing(id) && continue
            nbus = length(id)
            df = DataFrames.DataFrame(
                :name => "$(br)" .* string.(id),
                :r => get_nc_var(data, "$(br)r"),
                :x => get_nc_var(data, "$(br)x"),
                :b => get_nc_var(data, "$(br)b", zeros(nbus)),
                :s_nom => get_nc_var(
                    data,
                    "$(br)s_nom",
                    get_nc_var(data, "$(br)s_nom_opt", zeros(nbus)),
                ),
                :bus0 => BUS_PREFIX .* string.(get_nc_var(data, "$(br)bus0")),
                :bus1 => BUS_PREFIX .* string.(get_nc_var(data, "$(br)bus1")),
                :tap_position => get_nc_var(data, "$(br)tap_position", zeros(nbus)),
                :is_transformer => br == LINE_PREFIX ? falses(nbus) : trues(nbus),
            )
            push!(branches, df)
        end
        CSV.write(joinpath(out_path, "branch.csv"), vcat(branches...))
    end
end

function format_gen(src_file::AbstractString, out_path::AbstractString)
    NetCDF.open(src_file) do data

        name = GEN_PREFIX .* string.(get_nc_var(data, "generators_i"))
        ngen = length(name)
        generators_p_set = get_nc_var(data, "generators_p_set", zeros(ngen))
        generators_p_nom = get_nc_var(
            data,
            "generators_p_nom_opt",
            get_nc_var(data, "generators_p_nom", generators_p_set),
        )

        gens = DataFrames.DataFrame(
            :name => name,
            :bus => BUS_PREFIX .* string.(get_nc_var(data, "generators_bus")),
            :generators_p_set => generators_p_set,
            :generators_q_set => get_nc_var(data, "generators_q_set", zeros(ngen)),
            :generators_p_nom => generators_p_nom,
            :generators_p_min =>
                get_nc_var(data, "p_min_pu", zeros(ngen)) .* generators_p_nom,
            :generators_marginal_cost =>
                get_nc_var(data, "generators_marginal_cost", zeros(ngen)),
            :generators_carrier =>
                get_nc_var(data, "generators_carrier", repeat(["OCGT"], ngen)),
            :generators_min_up_time =>
                get_nc_var(data, "generators_min_up_time", zeros(ngen)),
            :generators_min_down_time =>
                get_nc_var(data, "generators_min_down_time", zeros(ngen)),
            :start_up_cost => get_nc_var(data, "start_up_cost", zeros(ngen)),
            :shut_down_cost => get_nc_var(data, "shut_down_cost", zeros(ngen)),
        )
        gens.zero .= 0.0
        gens.fuel = getfield.(map(x -> PYPSA_CATS[x], gens.generators_carrier), :fuel)
        gens.unit_type =
            getfield.(map(x -> PYPSA_CATS[x], gens.generators_carrier), :prime_mover)

        CSV.write(joinpath(out_path, "gen.csv"), gens)
    end
end

function format_load(src_file::AbstractString, out_path::AbstractString)
    NetCDF.open(src_file) do data
        name = LOAD_PREFIX .* string.(get_nc_var(data, "loads_i"))
        nload = length(name)
        loads = DataFrames.DataFrame(
            :name => name,
            :loads_bus => BUS_PREFIX .* string.(get_nc_var(data, "loads_bus")),
            :loads_q_set => get_nc_var(data, "loads_q_set", zeros(nload)),
            :loads_p_set => get_nc_var(data, "loads_p_set", zeros(nload)),
        )
        CSV.write(joinpath(out_path, "load.csv"), loads)
    end
end

function get_nc_ts(data, ts)
    ts_val = get_nc_var(data, ts)
    if isnothing(ts_val)
        return nothing
    end
    ts_name = (first(split(ts, "_")) * "_") .* get_nc_var(data, ts * "_i")
    df = DataFrames.DataFrame(Dict(zip(ts_name, eachrow(ts_val))))
    periods = get_nc_var(data, "snapshots")
    units =
        haskey(data.vars, "snapshots") ?
        get(data.vars["snapshots"].atts, "units", nothing) :
        get(data.gatts, "units", nothing)
    if isnothing(units)
        ref_date = Dates.today()
    else
        @assert occursin("days", units)
        ref_date =
            Dates.DateTime(replace(units, "days since " => ""), "yyyy-mm-dd HH:MM:SS")
    end
    df.DateTime = ref_date .+ Dates.Day.(periods)

    return df
end

function format_timeseries(src_file::AbstractString, out_path::AbstractString)
    tsp_dir = mkdir(joinpath(out_path, "timeseries"))
    tsp = []
    ts = Dict()
    NetCDF.open(src_file) do data
        for ts_id in ("loads_t_p", "generators_t_p")
            ts[ts_id] = get_nc_ts(data, "loads_t_p")
            (isnothing(ts[ts_id]) || DataFrames.nrow(ts[ts_id])) <= 1 && continue
            tsp_path = joinpath(tsp_dir, ts_id * ".csv")
            CSV.write(tsp_path, ts[ts_id])
            for i in names(ts[ts_id])
                i == "DateTime" && continue
                push!(
                    tsp,
                    Dict(
                        "category" => PYPSA_PSY_CAT[uppercasefirst(
                            first(split(ts_id, "_"))[1:end-1],
                        )],
                        "component_name" => i,
                        "property" => "Max Active Power",
                        "data_file" => joinpath("timeseries", ts_id * ".csv"),
                        "normalization_factor" => "max",
                        "resolution" => 3600,
                        "name" => "max_active_power",
                        "scaling_factor_multiplier_module" => "PowerSystems",
                        "scaling_factor_multiplier" => "get_max_active_power",
                    ),
                )
            end
        end
    end

    if length(tsp) > 0
        open(joinpath(out_path, "timeseries_pointers.json"), "w") do io
            JSON3.pretty(io, tsp)
        end
    end
end

end # module
