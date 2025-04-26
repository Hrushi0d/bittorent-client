import asyncio
import logging
import re

import aiohttp
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd


# Configure logging
def configure_logging():
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    return logging.getLogger('DHTLogAnalyzer')

logger = configure_logging()


def parse_log(filepath):
    """
    Parse DHT client log and extract send, timeout, and discovered peer entries.
    Returns:
        send_nodes: list of (ip, port, index)
        timeout_nodes: set of (ip, port)
        peer_nodes: list of (ip, port)
    """
    send_nodes = []
    timeout_nodes = set()
    peer_nodes = []
    with open(filepath, 'r') as f:
        lines = f.readlines()

    for idx, line in enumerate(lines):
        # record query index instead of timestamp
        if '[SEND]' in line:
            m = re.search(r"Sending get_peers to ([\d.]+):(\d+)", line)
            if m:
                ip, port = m.groups()
                send_nodes.append((ip, int(port), idx))
                logger.debug(f"Queued query to {ip}:{port} at index {idx}")
        elif '[ERROR]' in line and 'Timeout' in line:
            m = re.search(r"from \('([\d.]+)', (\d+)\)", line)
            if m:
                ip, port = m.groups()
                timeout_nodes.add((ip, int(port)))
                logger.debug(f"Recorded timeout for {ip}:{port}")
        elif '[DONE]' in line and idx + 1 < len(lines):
            # parse peers from next line
            for m in re.finditer(r"\('([\d.]+)', (\d+)\)", lines[idx + 1]):
                ip, port = m.groups()
                peer_nodes.append((ip, int(port)))
            logger.info(f"Parsed {len(peer_nodes)} total peers at log index {idx}")

    logger.info(f"Parsed log: {len(send_nodes)} queries, {len(timeout_nodes)} timeouts, {len(peer_nodes)} peers")
    return send_nodes, timeout_nodes, peer_nodes


# Timeline plot
def plot_timeline(send_nodes, timeout_nodes):
    """
    Plot a timeline of DHT queries and timeouts using query index as x-axis.
    """
    df = pd.DataFrame(send_nodes, columns=['ip', 'port', 'index'])
    df['type'] = df.apply(lambda r: 'timeout' if (r.ip, r.port) in timeout_nodes else 'query', axis=1)

    fig, ax = plt.subplots(figsize=(12, 6))
    for ttype, group in df.groupby('type'):
        color = 'red' if ttype == 'timeout' else 'blue'
        ax.scatter(group['index'], group['ip'], s=20, c=color, label=ttype)

    ax.set_xlabel('Log Line Index')
    ax.set_ylabel('DHT Node IP')
    ax.set_title('DHT Node Queries and Timeouts')
    ax.legend()
    # ax.grid(True)
    plt.tight_layout()
    plt.show()


# Network graph
def plot_network(send_nodes, peer_nodes):
    """
    Build and display a network graph connecting the client to queried nodes and discovered peers.
    """
    G = nx.Graph()
    G.add_node('Client', color='green')

    # query edges
    for ip, port, _ in send_nodes:
        G.add_edge('Client', f'{ip}:{port}')
    # peer edges
    for ip, port in peer_nodes:
        G.add_edge('Client', f'{ip}:{port}')  # connect peers directly to client

    colors = [G.nodes[n].get('color', 'skyblue') for n in G.nodes]
    pos = nx.spring_layout(G, seed=42, k=0.5)

    plt.figure(figsize=(14, 9))
    nx.draw(G, pos, with_labels=True, node_color=colors, node_size=200, font_size=8)
    plt.title('Client-Node-Peer Network Graph')
    plt.show()


# GeoIP with rate limiting and exponential backoff
async def fetch_geo(session, ip, sem, max_retries=3, base_backoff=1.0):
    url = f'http://ip-api.com/json/{ip}'
    backoff = base_backoff
    for attempt in range(1, max_retries + 1):
        async with sem:
            try:
                async with session.get(url, timeout=5) as resp:
                    # Force JSON parsing even if mimetype is unexpected
                    data = await resp.json(content_type=None)
                    if data.get('status') == 'success':
                        logger.debug(f"Geo data for {ip}: {data['lat']},{data['lon']}")
                        return ip, data['lat'], data['lon']
                    if resp.status == 429:
                        logger.warning(f"Rate limited for {ip}, retry {attempt}")
                    else:
                        logger.warning(f"Geo lookup failed {ip}, status {resp.status}")
            except Exception as e:
                logger.error(f"Error fetching geo for {ip}: {e}")
        await asyncio.sleep(backoff)
        backoff *= 2
    logger.error(f"Giving up on geo for {ip} after {max_retries} attempts")
    return None

async def gather_geo(ips, max_concurrent=5):
    sem = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_geo(session, ip, sem) for ip in ips]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r]


def plot_map(send_nodes, max_concurrent=5):
    """
    Plot geographic locations of queried DHT nodes.
    """
    ips = list({ip for ip, _, _ in send_nodes})
    geo_data = asyncio.run(gather_geo(ips, max_concurrent))
    if not geo_data:
        logger.warning("No geo data available to plot.")
        return

    df = pd.DataFrame(geo_data, columns=['ip', 'lat', 'lon'])
    fig = plt.figure(figsize=(15, 10))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.set_global()
    ax.scatter(df['lon'], df['lat'], color='red', s=30, transform=ccrs.PlateCarree())
    plt.title('Geolocation of Queried DHT Nodes')
    plt.show()


# ------------------------- Main -------------------------
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Analyze DHT client logs')
    parser.add_argument('logfile', help='Path to DHT log file')
    parser.add_argument('--no-network', action='store_true', help='Skip network graph')
    parser.add_argument('--no-map', action='store_true', help='Skip geographic map')
    parser.add_argument('--max-geo', type=int, default=5, help='Max concurrent geo lookups')
    args = parser.parse_args()

    send_nodes, timeout_nodes, peer_nodes = parse_log(args.logfile)
    logger.info(f"Loaded {len(send_nodes)} queries, {len(timeout_nodes)} timeouts, {len(peer_nodes)} peers")

    plot_timeline(send_nodes, timeout_nodes)
    if not args.no_network:
        plot_network(send_nodes, peer_nodes)
    if not args.no_map:
        print('help')
        # plot_map(send_nodes, max_concurrent=args.max_geo)
