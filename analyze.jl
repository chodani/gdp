using gdp, DataFrames, MethodChains, Deneb, DataSkimmer, UnicodePlots, CategoricalArrays, StatsBase
MethodChains.init_repl()

const inputp = "SAGDP"

readallgdp(inputp, "SAGDP2N")

## Look at files ##
readallgdp(inputp, "SAGDP2N") |> describe

describe(readallgdp(inputp, "SAGDP2N"), :eltype, :nuniqueall, :nmissing, :min, :max)
size(readallgdp(inputp, "SAGDP2N"))


CSV.read(map(IOBuffer, ["SAGDP/SAGDP2N_AK_1997_2023.csv", "SAGDP/SAGDP2N_AL_1997_2023.csv"]), DataFrame)

# Note there are state GDP files with the codes L (for < $50k in GDP), NM (for not meaningful), or D (not disclosed)
# These values will be converted to missings

# for "./data" in the line below,
# substitute the directory which holds the gdp data
const filenames = @mc readdir(abspath("./data"), join = true).{
    it[occursin.(r"SAGDP2N.*\.csv$", it)]
}

# see all filenames
show(stdout, "text/plain", filenames)

readgdps(filenames; footerskip = 4, missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"],
 pool = Dict(:TableName => true, :Unit => true))


@mc readgdps(filenames; footerskip = 4,
    missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"],
    pool = Dict(:TableName => true, :Unit => true)).{
        describe(it, :nunique, :nuniqueall, :nmissing, :eltype)
        }

# from general describe function we know...
#  year   mean   min   median      max   missing
# y1997, 25402.5, 0.0, 1849.25, 8.57755e6, 12
# y2023, 1.79528e5, 0.0, 22612.0, 2.73609e7, 6702

size(readgdps(filenames; footerskip = 4, missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"], pool = Dict(:TableName => true, :Unit => true)))
# size is (11048, 35)


# show all descriptions of industries in the dataframe
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"], 
	pool = Dict(:TableName => true, :Unit => true)).{
        unique(it.Description)
        show(stdout, "text/plain", it)
    }

# histogram of GDP distribution in the year 1997
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"], 
	pool = Dict(:TableName => true, :Unit => true)).{
	dropmissing!(it)
    histogram(it.y1997, title = "GDP 1997")
}

# barplot, can see region and only total GDP within region (1997)
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"], 
	pool = Dict(:TableName => true, :Unit => true)).{
	# dropmissing applies to all columns, but US is missing region tag...
	dropmissing!(it)
	#keep only all industry rows
	filter(row -> row.Description == "All industry total ", it)
	filter(row -> row.GeoName ∉ ["Southeast", "Far West", "Mideast", "Great Lakes", "Southwest",  "Plain", "New England", "Rocky Mountain"], it)
	sort(it, :y1997, rev = true)
    barplot(it.GeoName, it.y1997, title = "GDP 1997")
}

# barplot again, except for year 2023
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"], 
	pool = Dict(:TableName => true, :Unit => true)).{
	# dropmissing applies to all columns, but US is missing region tag...
	dropmissing!(it)
	#keep only all industry rows
	filter(row -> row.Description == "All industry total ", it)
	filter(row -> row.GeoName ∉ ["Southeast", "Far West", "Mideast", "Great Lakes", "Southwest",  "Plain", "New England", "Rocky Mountain"], it)
	sort(it, :y2023, rev = true)
    barplot(it.GeoName, it.y2023, title = "GDP 2023")
}

# barplot for gdp industries rank within Connecticut 2023
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"], 
	pool = Dict(:TableName => true, :Unit => true)).{
	dropmissing!(it)
	filter(row -> row.GeoName == "Connecticut", it)
	sort(it, :y2023, rev = true)
    barplot(it.Description, it.y2023, title = "GDP 2023")
}

# for 1997
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"], 
	pool = Dict(:TableName => true, :Unit => true)).{
	dropmissing!(it)
	filter(row -> row.GeoName == "Connecticut", it)
	sort(it, :y1997, rev = true)
    barplot(it.Description, it.y1997, title = "GDP 1997")
}

# testing to see if i need pooled vectors?
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"])

# first attempt to graph gdp throughout years
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
		# make sure stacked column is write type
    	stack(it, [:y1997, :y2023], variable_eltype=CategoricalValue{String})
    	transform!(it, :variable => x -> parse(Int64, x) => :variable)
    }

# total gdp in us throughout the years
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
    	filter(row -> row.Description ==  "All industry total ", it)
    	filter(row -> row.GeoName == "United States *", it)
    	# make sure stacked column is write type
    	stack(it, Between(:y1997, :y2023), variable_eltype=CategoricalValue{String})
    	Data(it) * Mark(:area) * Encoding(
        x=field("variable"),
        y=field("sum(value)"))
    }

# total gdp in us throughout the years (along with sub industries)
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
    	filter(row -> row.GeoName == "United States *", it)
    	# make sure stacked column is write type
    	stack(it, Between(:y1997, :y2023), variable_eltype=CategoricalValue{String})
    	Data(it) * Mark(:area, opacity = 0.3) * Encoding(
        x=field("variable"),
        y=field("sum(value)"),
        color="Description")
    }

# as line graphs
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
    	filter(row -> row.GeoName == "United States *", it)
    	# make sure stacked column is write type
    	stack(it, Between(:y1997, :y2023), variable_eltype=CategoricalValue{String})
    	Data(it) * Mark(:line, point=true) * Encoding(
        "variable",
        "sum(value)",
        color=:Description)
    }

# for the state of Connecticut
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
    	filter(row -> row.GeoName == "Connecticut", it)
    	filter(row -> row.Description ∉ ["All industry total ", " Private industries ", "Private services-providing industries 3/"], it)
    	# make sure stacked column is write type
    	stack(it, Between(:y1997, :y2023), variable_eltype=CategoricalValue{String})
    	Data(it) * Mark(:line, point=true) * Encoding(
        "variable",
        "sum(value)",
        color=:Description)
    }

# total gdp within regions throughout the years
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
    	filter(row -> row.Description ==  "All industry total ", it)
    	filter(row -> row.GeoName ∉ ["Southeast", "Far West", "Mideast", "Great Lakes", "Southwest",  "Plain", "New England", "Rocky Mountain", "United States *"], it)
    	# make sure stacked column is write type
    	stack(it, Between(:y1997, :y2023), variable_eltype=CategoricalValue{String})
    	Data(it) * Mark(:area) * Encoding(
        x=field("variable"),
        y=field("sum(value)"),
        color="GeoName")
    }

# total gdp within regions throughout the years, but as line graph
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
    	filter(row -> row.Description ==  "All industry total ", it)
    	filter(row -> row.GeoName ∉ ["Southeast", "Far West", "Mideast", "Great Lakes", "Southwest",  "Plain", "New England", "Rocky Mountain", "United States *"], it)
    	# make sure stacked column is write type
    	stack(it, Between(:y1997, :y2023), variable_eltype=CategoricalValue{String})
    	# tested log transform on gdp values
    	# transform!(it, :value => ByRow(log) => :value)
    	Data(it) * Mark(:line, point=true) * Encoding(
        "variable",
        "sum(value)",
        color=:GeoName)
    }

# try create ranking gdp within regions and see behavior across time period
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
		dropmissing!(it)
    	filter(row -> row.Description ==  "All industry total ", it)
    	filter(row -> row.GeoName ∉ ["Southeast", "Far West", "Mideast", "Great Lakes", "Southwest",  "Plain", "New England", "Rocky Mountain", "United States *"], it)
    	#transform(it, Between(:y1997, :y2023) => (x -> ByRow(ordinalrank(x))) => :rank)
    	transform(it, [ordinalrank(it[!, col]) for col in names(it, Between(:y1997, :y2023))] => :rank)}
    	
    	# make sure stacked column is write type
    	stack(it, Between(:y1997, :y2023), variable_eltype=CategoricalValue{String})
    	Data(it) * Mark(:area) * Encoding(
        x=field("variable"),
        y=field("sum(rank)"),
        color="GeoName")
    }    

# manual process of doing so...
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
		dropmissing!(it)
    	filter(row -> row.Description ==  "All industry total ", it)
    	filter(row -> row.GeoName ∉ ["Southeast", "Far West", "Mideast", "Great Lakes", "Southwest",  "Plain", "New England", "Rocky Mountain", "United States *"], it)
    	transform(it, :y1997 => (x -> ordinalrank(x)) => :rank1997)
    	transform(it, :y1998 => (x -> ordinalrank(x)) => :rank1998)
    	transform(it, :y1999 => (x -> ordinalrank(x)) => :rank1999)
    	transform(it, :y2000 => (x -> ordinalrank(x)) => :rank2000)
    	transform(it, :y2001 => (x -> ordinalrank(x)) => :rank2001)
    	transform(it, :y2002 => (x -> ordinalrank(x)) => :rank2002)
    	transform(it, :y2003 => (x -> ordinalrank(x)) => :rank2003)
    	transform(it, :y2004 => (x -> ordinalrank(x)) => :rank2004)
    	transform(it, :y2005 => (x -> ordinalrank(x)) => :rank2005)
    	transform(it, :y2006 => (x -> ordinalrank(x)) => :rank2006)
    	transform(it, :y2007 => (x -> ordinalrank(x)) => :rank2007)
    	transform(it, :y2008 => (x -> ordinalrank(x)) => :rank2008)
    	transform(it, :y2009 => (x -> ordinalrank(x)) => :rank2009)
		transform(it, :y2010 => (x -> ordinalrank(x)) => :rank2010)
		transform(it, :y2011 => (x -> ordinalrank(x)) => :rank2011)
		transform(it, :y2012 => (x -> ordinalrank(x)) => :rank2012)
		transform(it, :y2013 => (x -> ordinalrank(x)) => :rank2013)
		transform(it, :y2014 => (x -> ordinalrank(x)) => :rank2014)
		transform(it, :y2015 => (x -> ordinalrank(x)) => :rank2015)
		transform(it, :y2016 => (x -> ordinalrank(x)) => :rank2016)
		transform(it, :y2017 => (x -> ordinalrank(x)) => :rank2017)
		transform(it, :y2018 => (x -> ordinalrank(x)) => :rank2018)
		transform(it, :y2019 => (x -> ordinalrank(x)) => :rank2019)
		transform(it, :y2020 => (x -> ordinalrank(x)) => :rank2020)
		transform(it, :y2021 => (x -> ordinalrank(x)) => :rank2021)
		transform(it, :y2022 => (x -> ordinalrank(x)) => :rank2022)
		transform(it, :y2023 => (x -> ordinalrank(x)) => :rank2023)
    	# make sure stacked column is write type
    	stack(it, Between(:rank1997, :rank2023), variable_eltype=CategoricalValue{String})
    	Data(it) * Mark(:line, point=true) * Encoding(
        "variable",
        "sum(value)",
        color=:GeoName)
    } 


# tried creating subregional gdp list...
readgdps(filenames; footerskip = 4, 
	missingstring = ["", "(NA)", "(L)", "(NM)", "(D)"]).{
		select!(it, Not([:GeoFIPS, :Region, :TableName, :LineCode, :Unit]))
    	# make sure stacked column is write type
    	stack(it, Between(:y1997, :y2023), variable_eltype=CategoricalValue{String})
    	groupby(it, :GeoName)
    	[Data(region) * Mark(:line, point=true) * Encoding(
        "variable",
        "sum(value)") for region in it]
    }
