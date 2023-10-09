import pandas as pd
import requests
import streamlit as st
import json
import time

baseUrl = "https://yields.llama.fi"

protocols = requests.get(baseUrl +'/pools')

protocolDatast = pd.DataFrame.from_dict(protocols.json()["data"])
#protocolDatast = protocolData.loc[(protocolData['stablecoin'] == True)]

selected_chains = st.multiselect("Choose chain", protocolDatast.chain.unique())
all_chains = st.checkbox("Select all chains")

if all_chains:
    selected_chains = protocolDatast.chain.unique().tolist()

selected_projects = st.multiselect("Choose project", protocolDatast.project.unique())

all_projects = st.checkbox("Select all projects")


if all_projects:
    selected_projects = protocolDatast.project.unique().tolist()



protocolDatast = protocolDatast[protocolDatast['chain'].isin(selected_chains)]
protocolDatast = protocolDatast[protocolDatast['project'].isin(selected_projects)]
if (len(selected_chains) !=0) & (len(selected_projects) != 0):
    tv = st.number_input("Insert a TVL")
    protocolDatast = protocolDatast.loc[(protocolDatast['tvlUsd'] >= tv)]
    apy = st.number_input("Insert an APY")
    protocolDatast = protocolDatast.loc[(protocolDatast['apy'] >= apy)]
else:
    st.write("You should choose chains and projects")


st.title('DeFi Lama analysis')
show_data = st.expander("View Data")

with show_data:
    st.write(protocolDatast.head(20))

sel = st.selectbox("Options", protocolDatast.columns[3:])


chart_data_cache = {}

start_time = time.time()

def fetch_chart_data(pool_id):
    if pool_id in chart_data_cache:
        return chart_data_cache[pool_id]

    baseUrl3 = "https://yields.llama.fi/chart/"
    url = baseUrl3 + pool_id
    try:
        response = requests.get(url)
        time.sleep(1)
        response.raise_for_status()
        data = response.json()["data"]
        chart_data_cache[pool_id] = pd.DataFrame.from_dict(data)
        return chart_data_cache[pool_id]
    except (requests.exceptions.HTTPError, json.JSONDecodeError) as e:
        #st.write(f"Error: {e}")
        return None

def calculate_tvl(data, start, end):
    change_tvl = []

    for pool_id in data.pool:
        da = fetch_chart_data(pool_id)
        if da is not None and len(da) > (end - start):
            change_in_second_column = da["tvlUsd"].iloc[end] / da["tvlUsd"].iloc[start] - 1
            change_tvl.append(round(change_in_second_column * 100, 2))
        else:
            change_tvl.append(0)

    return change_tvl

remember = protocolDatast.sort_values(by=sel, ascending=False)
remember["tvlPct1D"] = calculate_tvl(protocolDatast.sort_values(by=sel, ascending=False), 0, 1)
remember["tvlPct7D"] = calculate_tvl(protocolDatast.sort_values(by=sel, ascending=False), 0, 7)
remember["tvlPct30D"] = calculate_tvl(protocolDatast.sort_values(by=sel, ascending=False), 0, 30)

end_time = time.time()

st.write(end_time-start_time)


unique_symbols = set()

for i in remember['symbol'].iloc[:].str.split('-'):
    unique_symbols.update(i)


remember['symbol'] = remember['symbol'].str.split('-')
st.write(remember)

selected_symbols  = st.multiselect("Symbols", unique_symbols)

filtered_data = remember[remember['symbol'].apply(lambda symbols_list: all(symbol in selected_symbols for symbol in symbols_list))]

st.write(filtered_data)

@st.cache_data
def convert_df(df):
    return df.to_csv().encode('utf-8')

new_csv = convert_df(protocolDatast)

st.download_button(
    label="Download data as CSV for all",
    data=new_csv,
    file_name='all_data.csv',
    mime='text/csv',
)



csv = convert_df(filtered_data)

st.download_button(
    label="Download data as CSV for selected Symbols",
    data=csv,
    file_name='df.csv',
    mime='text/csv',
)


st.caption('Then we can check by specific pool and date')


start_date = st.date_input("Select start date")
end_date = st.date_input("Select end date")

start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")




selected_symbols2 = st.multiselect("Choose symbols", protocolDatast.symbol.unique())


new_pool = protocolDatast.loc[protocolDatast['symbol'].isin(selected_symbols2)]


selected_chains2 = st.multiselect("Choose chain", new_pool.chain.unique())

new_pool = new_pool.loc[new_pool['chain'].isin(selected_chains2)]

selected_projects2 = st.multiselect("Choose project", protocolDatast.project.unique())

new_pool = new_pool.loc[new_pool['project'].isin(selected_projects2)]

st.write(new_pool)