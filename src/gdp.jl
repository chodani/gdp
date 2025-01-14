module gdp

using CSV, DataFrames, Dates, MethodChains, Serialization, Missings, CategoricalArrays
import TableTransforms: StdNames

export readallgdp, readgdps

"""
    readallgdp(inputpath, gdptype)

Read in a set of al gdp files into a single dataframe
"""
function readallgdp(inputpath, gdptype)
    resultdf = DataFrame()
    files = readdir(inputpath)
    for file in files
        input = joinpath(inputpath, file)
        if startswith(file, gdptype) & endswith(file, ".csv")
	        @mc CSV.read(input, DataFrame; footerskip = 4).{
	            resultdf = vcat(it, resultdf)
	        }
	    end
    end
    # many gdp values types are "InlineStrings.String15"
    for row in 1:nrow(resultdf)
    	for col in 9:35
    		println(resultdf[row, col])
    		println(typeof(resultdf[row, col]))
    		if passmissing(|)(resultdf[row, col] == "(L)", resultdf[row, col] == "(NM)")
    			resultdf[row, col] = 0
    		elseif resultdf[row, col] == "(NA)"
    			resultdf[row, col] = missing
    		end
    	end
    end
    #transform!(resultdf, [:"1997", :"1998", :"1999"] .=> (x -> parse.(Float64, x)), renamecols = false)
    return resultdf
end

"""
    readgdps(paths::Vector{String}; kwargs...)

Read a list of state gdp csvs, concatenate together, dump into one dataframe.
Do some initial dataframe processing.
"""
function readgdps(paths::Vector{String}; kwargs...)
    df = CSV.read(paths, DataFrame; kwargs...)
    nms = names(df, r"^\d")
    #println(nms)
    newnms = (str -> "y".*str)(nms)
    rename!(df, nms .=> newnms)
    transform!(df, [:GeoName, :IndustryClassification, :Description] .=> categorical, renamecols = false)
end

function readgdps(paths::Vector{String}; kwargs...)
    df = CSV.read(paths, DataFrame; kwargs...)
    # nms = names(df, r"^\d")
    # newnms = (str -> "y".*str)(nms)
    # rename!(df, nms .=> newnms)
    rename((str -> "y".*str), df, cols = contains(r"^\d"))
    # transform!(df, [:GeoName, :IndustryClassification, :Description] .=> categorical, renamecols = false)
end

end # module gdp
