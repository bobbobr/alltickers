import pandas as pd
import requests
import streamlit as st
import json
import time
import asyncio
import aiohttp

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


async def fetch_chart_data_async(_session, pool_id):
    baseUrl3 = "https://yields.llama.fi/chart/"
    url = baseUrl3 + pool_id
    async with _session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            return pd.DataFrame.from_dict(data.get("data", []))
        else:
            return None


async def fetch_all_chart_data(pool_ids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_chart_data_async(session, pool_id) for pool_id in pool_ids]
        return await asyncio.gather(*tasks)


async def fetch_all_chart_data_async(data):
    pool_ids = data.pool.tolist()
    chart_data = await fetch_all_chart_data(pool_ids)
    for i, pool_id in enumerate(pool_ids):
        chart_data_cache[pool_id] = chart_data[i]


@st.cache_data
async def fetch_and_calculate(pool_id, change_tvl, start, end):
    da = await fetch_chart_data_async(pool_id)
    if da is not None and len(da) > (end - start):
        change_in_second_column = da["tvlUsd"].iloc[end] / da["tvlUsd"].iloc[start] - 1
        change_tvl.append(round(change_in_second_column * 100, 2))
    else:
        change_tvl.append(0)


async def calculate_tvl_async(data, _session, start, end):
    change_tvl = []
    tasks = []

    for pool_id in data.pool:
        # Schedule the asynchronous fetch
        tasks.append(fetch_chart_data_async(_session, pool_id))


    # Await all tasks
    chart_data = await asyncio.gather(*tasks)

    for i, pool_id in enumerate(data.pool):
        da = chart_data[i]
        if da is not None and len(da) > (end - start):
            change_in_second_column = da["tvlUsd"].iloc[end] / da["tvlUsd"].iloc[start] - 1
            change_tvl.append(round(change_in_second_column * 100, 2))
        else:
            change_tvl.append(0)

    return change_tvl

remember = protocolDatast.sort_values(by=sel, ascending=False)

# Create an aiohttp ClientSession
async def main():
    async with aiohttp.ClientSession() as session:
        # Call calculate_tvl_async with the session
        remember["tvlPct1D"] = await calculate_tvl_async(protocolDatast.sort_values(by=sel, ascending=False), session, 0, 1)
        remember["tvlPct7D"] = await calculate_tvl_async(protocolDatast.sort_values(by=sel, ascending=False), session, 0, 7)
        remember["tvlPct30D"] = await calculate_tvl_async(protocolDatast.sort_values(by=sel, ascending=False), session, 0, 30)

# Run the main function
asyncio.run(main())

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

new_csv = convert_df(remember)

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
