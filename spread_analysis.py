#import libraries

import numpy as np
import pandas as pd

import os
import sys
import pickle

import plotly
import plotly.offline as pyo
import plotly.graph_objs as go

import calendar
from datetime import datetime
from calendar import monthrange
from datetime import timedelta
from datetime import date

import warnings
warnings.filterwarnings("ignore")

from arctic import Arctic, CHUNK_STORE

conn = Arctic('localhost')
lib_entsoe = conn['entsoe']

# function to change timezone from UTC to local time

def changing_timezone(x):
    ts = x.index.tz_localize('utc').tz_convert('Europe/Brussels')
    y = x.set_index(ts)
    return y.tz_localize(None)

# Read data
    
var1 = 'DayAheadPrices_12.1.D'
var2 = 'ActualTotalLoad_6.1.A'
var3 = 'AggregatedGenerationPerType_16.1.B_C'
var4 = 'PhysicalFlows_12.1.G'

country = []
dA_price = []
demand = []
gen = []

# Input 

print('Welcome to the Spread Analysis Tool.')
print('Enter the countries and date range below. Note: Spread = A - B')
ip_1 = input("Enter country A --> DE/FR/BE/ES/IT/PL) : ")
ip_2 = input("Enter country B  --> DE/FR/BE/ES/IT/PL) : ")

ref_start_date = input("Enter start date (dd/mm/yyyy): ")
ref_end_date = input("Enter end date (dd/mm/yyyy): ")

start_date = datetime.strptime(ref_start_date, '%d/%m/%Y') + timedelta(days = - 1)
end_date = datetime.strptime(ref_end_date, '%d/%m/%Y') + timedelta(days = 1)

list_countries = [ip_1,ip_2]

if 'DE' in list_countries:
    pass
else:
    list_countries.append('DE')
    
for i in range(len(list_countries)):
        dA_price.append(lib_entsoe.read(var1 + '_' + list_countries[i], chunk_range=pd.date_range(start_date, end_date)))
        demand.append(lib_entsoe.read(var2 + '_' + list_countries[i], chunk_range=pd.date_range(start_date, end_date)))
        gen.append(lib_entsoe.read(var3 + '_' + list_countries[i], chunk_range=pd.date_range(start_date, end_date)))

df_1 = pd.concat(dA_price,axis=1)
df_2 = pd.concat(demand,axis=1)
df_3 = pd.concat(gen,axis=1)

# convert 15 min data to hourly data
df_1 = df_1.resample('H').mean() 
df_2 = df_2.resample('H').mean()       
df_3 = df_3.resample('H').mean()

# Read cross border flows

flows = []

for i in [ip_1,ip_2]:
    df_exports = pd.DataFrame(columns=[])
    df_imports = pd.DataFrame(columns=[])
    
    if i == 'DE':
        interco = ['AT','BE','CZ','DK','FR','LU','NL','PL', 'SE','CH']
    elif i == 'FR':
        interco = ['BE','DE','IT','ES','CH', 'GB']
    elif i == 'BE':
        interco = ['FR','DE','LU','NL', 'GB']
    elif i == 'ES':
        interco = ['FR','PT']
    elif i == 'IT':
        interco = ['AT','GR','FR','MT','ME','SI','CH']
    elif i == 'NL':
        interco = ['BE','DK','DE','NO','GB']
    elif i == 'PL':
        interco = ['CZ','DE','LT','SK','SE']
    #elif i == 'GB':
        #interco = ['BE','FR','IE','NL']
    
    for j in interco:
        # exports
        prefix = var4 + '_' + i + '_' + j
        try:
            out_flows = lib_entsoe.read(prefix, chunk_range=pd.date_range(start_date, end_date))
            df_exports = pd.merge(df_exports,out_flows ,how='outer',right_index=True, left_index=True)    
        except Exception:
            pass    
        # exports
        prefix = var4 + '_' + j + '_' + i
        try:
            in_flows = lib_entsoe.read(prefix, chunk_range=pd.date_range(start_date, end_date))
            df_imports = pd.merge(df_imports,in_flows ,how='outer',right_index=True, left_index=True) 
        except Exception:
            pass
        
    flows.append(df_imports.subtract(df_exports.values).sum(axis = 1, skipna= True))
    
df_4 = pd.concat(flows,axis=1)
df_4 = df_4.resample('H').mean()
df_4.columns= ['Net_Imports_'+ ip_1,'Net_Imports_'+ ip_2]

# merging data to a single dataframe

df_merge = pd.DataFrame(columns=[])

for df in [df_1,df_2,df_3,df_4]:
    df_merge = pd.merge(df_merge, df,how='outer',right_index=True, left_index=True)

# changing timezones 
df_merge = changing_timezone(df_merge)

df_merge['Spread']=df_merge[df_merge.columns[0]]-df_merge[df_merge.columns[1]]

df_merge = df_merge[df_merge.columns.drop(list(df_merge.filter(regex='ActualConsumption')))]

df_merge=df_merge.loc[(df_merge.index>=datetime.strptime(ref_start_date, '%d/%m/%Y'))&(df_merge.index<end_date)]

def create_plot(
    title = None,
    df = None,
    countries_code = None,
    gen_types = None,
    gen_code = None,
    list_countries = None
    ):
    
    from plotly.subplots import make_subplots
    
    # Define Subplots
    
    fig = plotly.subplots.make_subplots(
        rows=len(list_countries)+1, cols=1, 
        subplot_titles = (
            'Spot Price & Spread',
            list_countries[0] + ' Generation',
            list_countries[1] + ' Generation',
            'DE RES Generation'
            ),
        shared_xaxes=True,
        vertical_spacing=0.1,
        specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}],[{"secondary_y": False}]] if (len(list_countries) == 3) else [[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]]
)
    
    #-----------------------------------------------------------------------------
    
    var = 'DayAheadPrices'
    for col in [ip_1,ip_2]:
        trace = go.Scatter(
            x = df.index, 
            y = df[var+'_'+col], 
            name = col,
            line_color = countries_code[col])
        fig.append_trace(trace, 1, 1)
        
    #-----------------------------------------------------------------------------
        
    trace = go.Bar(
        x = df.index, 
        y = df['Spread'], 
        name = list_countries[0] + '-' + list_countries[1],
        opacity = 0.5
    )
    fig.add_trace(trace, 1, 1,  secondary_y=True)
    
    #-----------------------------------------------------------------------------
    
    # Generation
    var = 'ActualGenerationOutput'
    gen_list = []
    for col in gen_types:
        try:
            trace = go.Bar(
                x = df.index, 
                y = df[var + ' ' + list_countries[0] + ' ' + col], 
                name = gen_code[col]["name"],
                marker_color = gen_code[col]["colour"],
                hovertemplate='%{x},%{y:.1f}',
                legendgroup = col
                )
            gen_list.append(col)
            fig.append_trace(trace, 2, 1)
        except KeyError:
            pass
    
    # CrossBorder Trade
    
    trace = go.Bar(
                x = df.index, 
                y = df['Net_Imports'+'_'+list_countries[0]], 
                name = 'CrossBorder Trade',
                marker_color = 'orchid',
                hovertemplate='%{x},%{y:.1f}',
                legendgroup = 'g1',
                showlegend=False
                )
    fig.add_trace(trace, 2, 1)
        
    # Demand
    
    trace = go.Scatter(
        x = df.index, 
        y = df['ActualTotalLoad'+'_'+list_countries[0]], 
        name = 'Demand',
        visible = 'legendonly',
        line = dict(color='black', width=3),
        hovertemplate='%{x},%{y:.1f}',
        legendgroup = 'g2',
        showlegend=False

    )
    fig.add_trace(trace, 2, 1)
    
    #-----------------------------------------------------------------------------
    
    # Generation
                                        
    for col in gen_types:
        try:
            trace = go.Bar(
                x = df.index, 
                y = df[var + ' ' + list_countries[1] + ' ' + col], 
                name = gen_code[col]["name"],
                marker_color = gen_code[col]["colour"],
                hovertemplate='%{x},%{y:.1f}',
                legendgroup = col,
                showlegend=False if col in gen_list else True
                )
            fig.append_trace(trace, 3, 1)
        except KeyError:
            pass
    
    # CrossBorder Trade
    
    trace = go.Bar(
                x = df.index, 
                y = df['Net_Imports'+'_'+list_countries[1]], 
                name = 'CrossBorder Trade',
                marker_color = 'orchid',
                hovertemplate='%{x},%{y:.1f}',
                legendgroup = 'g1',
                )
    fig.add_trace(trace, 3, 1)
        
    # Demand
    
    trace = go.Scatter(
        x = df.index, 
        y = df['ActualTotalLoad'+'_'+list_countries[1]], 
        name = 'Demand',
        visible = 'legendonly',
        line = dict(color='black', width=3),
        hovertemplate='%{x},%{y:.1f}',
        legendgroup = 'g2'
    )
    fig.add_trace(trace, 3, 1)

    #-----------------------------------------------------------------------------
    
    # RES Generation DE
                                        
    if 'DE' in [ip_1,ip_2]:
        pass
    else:
        RES_gen_types = ['Wind Offshore','Wind Onshore','Solar']          
        for col in RES_gen_types:
            trace = go.Bar(
                    x = df.index, 
                    y = df[var + ' ' + 'DE' + ' ' + col], 
                    name = gen_code[col]["name"],
                    marker_color = gen_code[col]["colour"],
                    hovertemplate='%{x},%{y:.1f}',
                    legendgroup = col,
                    showlegend=False if col in gen_list else True
                    )
            fig.append_trace(trace, 4, 1)
       
    #----------------------------------------------------------------------------
  

    # Layout
    
    fig.update_layout(
        title_text = title,
        barmode='relative',
        bargap=0,
        
        yaxis1 = dict(
            anchor = "x",
            autorange = True,
            title_text = "€/MWh"
            
        ),
        
        yaxis2 = dict(
            anchor = "x",
            autorange = True,
            title_text = "€/MWh",
            side = 'right',
        ),
        
        yaxis3 = dict(
            anchor = "x",
            autorange = True,
            title_text = "MWh/h",
        ),
        
         yaxis4 = dict(
            anchor = "x",
            autorange = True,
            title_text = "MWh/h",
        ),
          yaxis5 = dict(
            anchor = "x",
            autorange = True,
            title_text = "MWh/h",
        )
            

        )
    
    return fig


countries_dict = {
  "DE": "indianred",
  "FR": "royalblue",
  "BE": "rosybrown",
  "ES": "tomato",
  "IT": "green",
  "NL": "orange",
  "GB": "navy",
  "AT": "coral",
  "CZ": "firebrick",
  "CH": "lawngreen",
  "DK": "teal",
  "LU": "orchid",
  "PL": "silver",
  "PT": "darkgreen",
  "IE": "pink",
  "GR": "azure",
  "NO": "orangered",
  "SE": "thistle",
  "SK": "salmon",
  "LT": "purple",
  "MT": "olive",
  "SI": "crimson",
  "ME": "gold",
    
}

gen_tech_dict = { 
    "Nuclear" : {
        'name' : 'Nuclear',
        'colour' : 'indianred'
    },
    "Biomass" : {
        'name' : 'Biomass',
        'colour' : 'darkgreen'
    },
     "Fossil Hard coal" : {
        'name' : 'Hard Coal',
        'colour' : 'brown'
    },
     "Fossil Brown coal/Lignite" : {
        'name' : 'Lignite',
        'colour' : 'saddlebrown'
    },
     "Fossil Gas" : {
        'name' : 'CCGT',
        'colour' : 'silver'
    },
     "Hydro Run-of-river and poundage" : {
        'name' : 'Hydro R-o-R',
        'colour' : 'blue'
    },
     "Hydro Pumped Storage" : {
        'name' : 'Pumped Storage',
        'colour' : 'orange'
    },
     "Hydro Water Reservoir" : {
        'name' : 'Hydro Reservoir',
        'colour' : 'plum'
    },
     "Solar" : {
        'name' : 'Solar',
        'colour' : 'gold'
    },
     "Wind Offshore" : {
        'name' : 'Wind Offshore',
        'colour' : 'green'
    },
     "Wind Onshore" : {
        'name' : 'Wind Onshore',
        'colour' : 'steelblue'
    },
    
}
    
fig = create_plot(
    
    title = 'Spread_Analysis:' + list_countries[0]+ '-' + list_countries[1],
    
    df = df_merge,
    
    countries_code = countries_dict,
                    
    gen_types = [

         'Nuclear',
         'Biomass',
         'Hydro Run-of-river and poundage',
         'Hydro Water Reservoir',
        
         'Fossil Hard coal',
         'Fossil Gas',
         'Fossil Brown coal/Lignite',
         'Hydro Pumped Storage',
        
         'Wind Offshore',
         'Wind Onshore',
         'Solar'
                      
    ],
    
    gen_code = gen_tech_dict,
    
    list_countries = list_countries,

)

outfile = list_countries[0]+ '-' + list_countries[1] + '.html'

plotly.offline.plot(fig, filename = os.path.join(os.getcwd() + '/plots', outfile))