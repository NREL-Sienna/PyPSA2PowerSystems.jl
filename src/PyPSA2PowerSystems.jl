module PyPSA2PowerSystems

import PowerSystems
import NetCDF
import DataFrames
import CSV

export System
export format_pypsa

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
    return out_path
end

function get_nc_var(data::NetCDF.NcFile, var::String, default = nothing)
    return haskey(data.vars, var) ? NetCDF.readvar(data[var]) : default
end

function format_bus(src_file::AbstractString, out_path::AbstractString)
    NetCDF.open(src_file) do data

        id = parse.(Int, get_nc_var(data, "buses_i"))
        name = "bus_" .* string.(id)
        v_nom = get_nc_var(data, "buses_v_nom")
        v_mag_pu_set = get_nc_var(data, "buses_v_mag_pu_set", ones(length(id)))
        control = get_nc_var(data, "buses_control", repeat(["PV"], length(id)))
        buses = DataFrames.DataFrame(
            :name => name,
            :v_nom => v_nom,
            :v_mag_pu_set => v_mag_pu_set,
            :control => control,
            :id => id,
        )
        CSV.write(joinpath(out_path, "bus.csv"), buses)
    end
end

function format_branch(src_file::AbstractString, out_path::AbstractString)
    NetCDF.open(src_file) do data
        branches = []
        for br in ("line", "transformer")
            id = get_nc_var(data, "$(br)s_i")
            nbus = length(id)
            df = DataFrames.DataFrame(
                :name => "$(br)_" .* string.(id),
                :r => get_nc_var(data, "$(br)s_r"),
                :x => get_nc_var(data, "$(br)s_x"),
                :b => get_nc_var(data, "$(br)s_b", zeros(nbus)),
                :s_nom => get_nc_var(
                    data,
                    "$(br)s_s_nom",
                    get_nc_var(data, "$(br)s_s_nom_opt", zeros(nbus)),
                ),
                :bus0 => "bus_" .* string.(get_nc_var(data, "$(br)s_bus0")),
                :bus1 => "bus_" .* string.(get_nc_var(data, "$(br)s_bus1")),
                :tap_position => get_nc_var(data, "$(br)s_tap_position", zeros(nbus)),
                :is_transformer => br == "line" ? falses(nbus) : trues(nbus),
            )
            push!(branches, df)
        end
        CSV.write(joinpath(out_path, "branch.csv"), vcat(branches...))
    end
end

function format_gen(src_file::AbstractString, out_path::AbstractString)
    NetCDF.open(src_file) do data

        name = "gen_" .* string.(get_nc_var(data, "generators_i"))
        ngen = length(name)
        generators_p_set = get_nc_var(data, "generators_p_set", zeros(ngen))
        generators_p_nom = get_nc_var(
            data,
            "generators_p_nom_opt",
            get_nc_var(data, "generators_p_nom", generators_p_set),
        )

        gens = DataFrames.DataFrame(
            :name => name,
            :bus => "bus_" .* string.(get_nc_var(data, "generators_bus")),
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
        gens.fuel = replace.(gens.generators_carrier, "OCGT" => "NG")
        gens.unit_type = replace(gens.generators_carrier, "OCGT" => "GT")

        CSV.write(joinpath(out_path, "gen.csv"), gens)
    end
end

function format_load(src_file::AbstractString, out_path::AbstractString)
    NetCDF.open(src_file) do data

        name = "loads_" .* string.(get_nc_var(data, "loads_i"))
        nload = length(name)
        loads = DataFrames.DataFrame(
            :name => name,
            :loads_bus => "bus_" .* string.(get_nc_var(data, "loads_bus")),
            :loads_q_set => get_nc_var(data, "loads_q_set", zeros(nload)),
            :loads_p_set => get_nc_var(data, "loads_p_set", zeros(nload)),
        )
        CSV.write(joinpath(out_path, "load.csv"), loads)
    end
end

end # module
