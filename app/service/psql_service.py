import io
import json
from math import isnan
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import folium
from folium import plugins
import seaborn as sns
from app.repository.psql_repository import get_locations_for_common_attacks
from toolz import pipe, curry
from typing import List, Tuple

def create_map(center=None, zoom=2):
    if center is None:
        center = [0, 0]
    return folium.Map(
        location=center,
        zoom_start=zoom,
        tiles='CartoDB positron'
    )
@curry
def validate_coordinates(lat: float, lon: float) -> bool:
    return (isinstance(lat, (int, float)) and
            isinstance(lon, (int, float)) and
            not (np.isnan(lat) or np.isnan(lon)) and
            -90 <= lat <= 90 and
            -180 <= lon <= 180 and
            lat != 0 and lon != 0)
@curry
def filter_valid_results(results: List[Tuple]) -> List[Tuple]:
    return [
        (region, count, score, lat, lon)
        for region, count, score, lat, lon in results
        if validate_coordinates(lat, lon)
    ]
@curry
def create_circle_marker(m: folium.Map, region: str, count: int,
                        score: float, lat: float, lon: float) -> folium.Map:
    avg_casualties = score / count if count > 0 else 0
    folium.Circle(
        location=[lat, lon],
        radius=avg_casualties * 100000,
        color='red',
        fill=True,
        popup=f"{region}<br>Avg Casualties: {avg_casualties:.2f}<br>Total Events: {count}"
    ).add_to(m)
    return m
def create_base_map(center: List[float] = None, zoom: int = 2) -> folium.Map:
    """Create a base Folium map with consistent settings."""
    return folium.Map(
        location=center or [0, 0],
        zoom_start=zoom,
        tiles='CartoDB positron'
    )
# 1
def deadliest_attacks_service(results):
    df = pd.DataFrame(results, columns=['attack_type', 'casualty_score'])
    plt.figure(figsize=(10, 6))
    plt.bar(df['attack_type'], df['casualty_score'])
    plt.xticks(rotation=45, ha='right')
    plt.title('Deadliest Attack Types')
    plt.ylabel('Casualty Score (Killed×2 + Wounded)')
    plt.xlabel('Attack Type')
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf
# 2
def casualties_by_region_service(results: List[Tuple]) -> io.BytesIO:
    buf = io.BytesIO()
    pipe(
        results,
        filter_valid_results,
        lambda data: create_base_map(),
        lambda m: add_all_markers(m, filter_valid_results(results)),
        lambda m: m.save(buf, close_file=False)
    )
    return buf
def add_all_markers(m: folium.Map, data: List[Tuple]) -> folium.Map:
    for result in data:
        m = create_circle_marker(m, *result)
    return m
# 3
def top_casualty_groups_service(results):
    plt.figure(figsize=(10, 6))
    plt.bar([r[0] for r in results], [r[1] for r in results])
    plt.title('Top 5 Most Lethal Terrorist Groups')
    plt.xlabel('Group Name')
    plt.ylabel('Total Casualties')
    plt.xticks(rotation=45)
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf
# 4
def attack_target_correlation_service(results):
    df = pd.DataFrame(results, columns=['attack_type', 'target_type', 'event_count'])
    correlation_matrix = df.pivot_table(
        values='event_count',
        index='attack_type',
        columns='target_type'
    ).corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
    plt.title('Attack-Target Type Correlation')
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf
# 5
def attack_trends_service(annual_trends,monthly_trends,year):
    plt.figure(figsize=(15, 10))
    plt.subplot(2, 1, 1)
    plt.bar([str(trend.year) for trend in annual_trends],
            [trend.attack_count for trend in annual_trends])
    plt.title('Annual Attack Trends')
    plt.xlabel('Year')
    plt.ylabel('Number of Attacks')
    plt.xticks(rotation=45)
    plt.subplot(2, 1, 2)
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_counts = [next((trend.attack_count for trend in monthly_trends if trend.month == i + 1), 0)
                    for i in range(12)]
    plt.bar(months, month_counts)
    plt.title(f'Monthly Attack Trends in {year}')
    plt.xlabel('Month')
    plt.ylabel('Number of Attacks')
    plt.xticks(rotation=45)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf
# 6
def attack_change_by_region_service(df,top_n):
    df['percent_change'] = ((df['current_attacks'] - df['previous_attacks']) / df['previous_attacks'] * 100).fillna(0)
    top_regions = df.groupby('region')['percent_change'].mean().abs().nlargest(top_n)
    plt.figure(figsize=(12, 6))
    plt.bar(top_regions.index, top_regions.values)
    plt.title(f'Top {top_n} Regions - Attack Percentage Change')
    plt.xlabel('Region')
    plt.ylabel('Average Percentage Change')
    plt.xticks(rotation=45)
    for i, v in enumerate(top_regions.values):
        plt.text(i, v, f'{v:.2f}%', ha='center', va='bottom')
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf
# 7
def terror_heatmap_service(locations, current_year, time_period, region_filter):
    m = create_map()

    # Debug: Print incoming data
    print(f"Received {len(locations)} locations for processing")

    def is_valid_coord(lat, lon):
        try:
            lat, lon = float(lat), float(lon)
            return (not isnan(lat) and not isnan(lon) and
                    -90 <= lat <= 90 and -180 <= lon <= 180 and
                    lat != 0 and lon != 0)  # Exclude 0,0 coordinates
        except (ValueError, TypeError):
            return False

    if time_period in ['3_years', '5_years']:
        years_data = {}
        years_range = range(current_year - (3 if time_period == '3_years' else 5), current_year + 1)

        for year in years_range:
            year_locations = [
                [float(loc.latitude), float(loc.longitude), float(loc.event_count)]
                for loc in locations
                if loc.year == year and is_valid_coord(loc.latitude, loc.longitude)
            ]
            if year_locations:
                years_data[year] = year_locations
                print(f"Year {year}: Found {len(year_locations)} valid locations")

        if years_data:
            try:
                plugins.HeatMapWithTime(
                    list(years_data.values()),
                    index=list(years_data.keys()),
                    auto_play=True,
                    max_opacity=0.8,
                    radius=15
                ).add_to(m)
                print("Successfully added HeatMapWithTime")
            except Exception as e:
                print(f"Error creating time heatmap: {str(e)}")
                # Fallback to regular heatmap
                all_heat_data = [
                    [float(loc.latitude), float(loc.longitude), float(loc.event_count)]
                    for loc in locations
                    if is_valid_coord(loc.latitude, loc.longitude)
                ]
                if all_heat_data:
                    plugins.HeatMap(
                        all_heat_data,
                        name='Terror Hotspots',
                        max_opacity=0.8,
                        radius=15
                    ).add_to(m)
                    print("Added fallback HeatMap")
    else:
        heat_data = [
            [float(loc.latitude), float(loc.longitude), float(loc.event_count)]
            for loc in locations
            if is_valid_coord(loc.latitude, loc.longitude)
        ]
        print(f"Regular heatmap: Found {len(heat_data)} valid locations")

        if heat_data:
            plugins.HeatMap(
                heat_data,
                name='Terror Hotspots',
                max_opacity=0.8,
                radius=15
            ).add_to(m)
            print("Added regular HeatMap")

    # Add layer control
    folium.LayerControl().add_to(m)

    # Calculate stats
    total_events = sum(loc.event_count for loc in locations)
    valid_locations = len([loc for loc in locations if is_valid_coord(loc.latitude, loc.longitude)])

    stats_html = f"""
        <div style='position: fixed; 
                    bottom: 50px; 
                    left: 50px; 
                    z-index: 1000;
                    background-color: white;
                    padding: 10px;
                    border: 2px solid #ccc;
                    border-radius: 5px;'>
            <h4>Terror Hotspots Analysis</h4>
            <p><b>Total Events:</b> {total_events}</p>
            <p><b>Unique Locations:</b> {valid_locations}</p>
            <p><b>Time Period:</b> {time_period.replace('_', ' ').title()}</p>
            {'<p><b>Region:</b> ' + region_filter + '</p>' if region_filter else ''}
            <p style='font-size: 0.8em; color: #666;'>
                Heatmap intensity indicates number of events
            </p>
        </div>
    """

    m.get_root().html.add_child(folium.Element(stats_html))

    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf
# 8
def active_groups_heatmap_service(results, region_filter):
    m = create_map()

    regions_data = {}
    for r in results:
        if isinstance(r, dict):
            region = r['region_name']
            coords = {'lat': r['avg_lat'], 'lon': r['avg_lon']}
            group_data = {'name': r['group_name'], 'count': r['attack_count']}
        else:
            region = region_filter
            coords = {'lat': r.avg_lat, 'lon': r.avg_lon}
            group_data = {'name': r.group_name, 'count': r.attack_count}

        if region not in regions_data:
            regions_data[region] = {
                'coords': coords,
                'groups': []
            }
        regions_data[region]['groups'].append(group_data)

    for region, data in regions_data.items():
        if data['coords']['lat'] and data['coords']['lon']:
            popup_content = f"""
                <div style='min-width: 250px'>
                    <h4>Active Groups in {region}</h4>
                    <table style='width:100%'>
                        <tr><th>Group</th><th>Attacks</th></tr>
                        {''.join(f"<tr><td>{g['name']}</td><td>{g['count']}</td></tr>"
                                 for g in data['groups'])}
                    </table>
                </div>
            """

            folium.Marker(
                location=[data['coords']['lat'], data['coords']['lon']],
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Top groups in {region}",
                icon=folium.Icon(color='red', icon='info-sign')
            ).add_to(m)

    summary = f"<div style='position:fixed;bottom:50px;left:50px;background:white;padding:10px;border:2px solid #ccc;border-radius:5px;z-index:1000'>"
    summary += f"<h4>Active Groups Analysis</h4>"
    summary += f"<p>Showing top 5 active groups {'in ' + region_filter if region_filter else 'per region'}</p>"
    summary += "</div>"

    m.get_root().html.add_child(folium.Element(summary))

    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf
# 9
def perpetrators_casualties_correlation_service(results):
    df = pd.DataFrame(results, columns=['event_id', 'perpetrator_count', 'total_casualties'])
    df = df[(df['perpetrator_count'] > 0) & (df['total_casualties'] > 0)]
    if len(df) < 2:
        plt.figure(figsize=(10, 6))
        plt.text(0.5, 0.5, 'Insufficient data for correlation analysis',
                 horizontalalignment='center', verticalalignment='center')
        plt.title('Perpetrators vs Casualties Correlation')
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()
        return buf
    try:
        correlation = df['perpetrator_count'].corr(df['total_casualties'])
    except Exception:
        correlation = 0
    plt.figure(figsize=(12, 6))
    plt.scatter(df['perpetrator_count'], df['total_casualties'],
                alpha=0.6,
                c=df['perpetrator_count'],
                cmap='viridis')
    try:
        m, b = np.polyfit(df['perpetrator_count'], df['total_casualties'], 1)
        x_line = np.linspace(df['perpetrator_count'].min(), df['perpetrator_count'].max(), 100)
        plt.plot(x_line, m * x_line + b, color='red', linestyle='--', label='Trend Line')
    except Exception:
        print("Could not fit regression line")
    plt.colorbar(label='Perpetrator Count')
    plt.title(f'Perpetrators vs Casualties Correlation\nCorrelation Coefficient: {correlation:.4f}')
    plt.xlabel('Number of Perpetrators')
    plt.ylabel('Total Casualties')
    plt.legend()
    stats_text = f"""
        Correlation: {correlation:.4f}
        Data Points: {len(df)}
        Perpetrators (Avg): {df['perpetrator_count'].mean():.2f}
        Casualties (Avg): {df['total_casualties'].mean():.2f}
        """
    plt.annotate(stats_text,
                 xy=(0.05, 0.95),
                 xycoords='axes fraction',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8),
                 verticalalignment='top')
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf
# 10
def events_casualties_correlation_service(results,region_name):
    df = pd.DataFrame(results, columns=['region', 'event_count', 'total_casualties'])
    correlation = df['event_count'].corr(df['total_casualties'])
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(
        df['event_count'],
        df['total_casualties'],
        c=df['total_casualties'],
        s=df['event_count'],
        alpha=0.6,
        cmap='viridis'
    )
    plt.colorbar(scatter, label='Total Casualties')
    m, b = np.polyfit(df['event_count'], df['total_casualties'], 1)
    plt.plot(df['event_count'], m * df['event_count'] + b, color='red', linestyle='--')
    plt.title(f'Event Count vs Casualties Correlation\nby {"Specific Region" if region_name else "All Regions"}')
    plt.xlabel('Number of Events')
    plt.ylabel('Total Casualties')
    for _, row in df.iterrows():
        plt.annotate(row['region'],
                     (row['event_count'], row['total_casualties']),
                     xytext=(5, 5),
                     textcoords='offset points',
                     fontsize=8)
    plt.annotate(f'Correlation: {correlation:.4f}',
                 xy=(0.05, 0.95),
                 xycoords='axes fraction',
                 bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf
# 11
def groups_common_goals_service(results, region_filter=None, country_filter=None):
    m = create_map()
    location_groups = {}
    for result in results:
        group_name, target_type, region, country, count, lat, lon = result
        if lat and lon:
            key = (lat, lon, target_type)
            if key not in location_groups:
                location_groups[key] = {
                    'groups': [],
                    'count': 0,
                    'region': region,
                    'country': country,
                    'target_type': target_type
                }
            location_groups[key]['groups'].append({
                'name': group_name,
                'attack_count': count
            })
            location_groups[key]['count'] += count
    for (lat, lon, target_type), data in location_groups.items():
        if len(data['groups']) > 1:
            popup_html = f"""
                <div style='min-width: 200px'>
                    <h4>Common Target: {target_type}</h4>
                    <p><b>Region:</b> {data['region']}</p>
                    <p><b>Country:</b> {data['country']}</p>
                    <p><b>Number of Groups:</b> {len(data['groups'])}</p>
                    <p><b>Total Attacks:</b> {data['count']}</p>
                    <hr>
                    <h5>Groups:</h5>
                    <ul>
            """
            for group in sorted(data['groups'], key=lambda x: x['attack_count'], reverse=True):
                popup_html += f"<li>{group['name']} ({group['attack_count']} attacks)</li>"
            popup_html += "</ul></div>"
            folium.CircleMarker(
                location=[lat, lon],
                radius=min(20, len(data['groups']) * 3),
                color='red',
                fill=True,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{len(data['groups'])} groups targeting {target_type}"
            ).add_to(m)
    summary_html = f"""
    <div style='position: fixed; 
                bottom: 50px; 
                left: 50px; 
                z-index: 1000;
                background-color: white;
                padding: 10px;
                border: 2px solid #ccc;
                border-radius: 5px;'>
        <h4>Groups with Common Goals Analysis</h4>
        <p>Filter: {region_filter or country_filter or 'None'}</p>
        <p>Total locations: {len(location_groups)}</p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(summary_html))
    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf
# 12
def group_activity_expansion_service(results):
    m = create_map()
    for group_name, expansions_json, region_count in results:
        # Parse and validate expansions data
        expansions = []
        for exp_json in expansions_json:
            try:
                exp = json.loads(exp_json)
                if exp['lat'] and exp['lon']:  # Only include valid coordinates
                    expansions.append(exp)
            except (json.JSONDecodeError, KeyError):
                continue

        if not expansions:  # Skip if no valid expansions
            continue

        # Sort by year
        expansions.sort(key=lambda x: x['year'])

        # Calculate expansion years
        expansion_years = max(exp['year'] for exp in expansions) - min(exp['year'] for exp in expansions)

        # Create a feature group for this terrorist group
        fg = folium.FeatureGroup(name=f"{group_name} ({region_count} regions)")

        # Track valid coordinates for drawing lines
        valid_coords = []

        # Add markers for each location
        for i, exp in enumerate(expansions):
            lat, lon = exp['lat'], exp['lon']

            # Create marker
            color = f'hsl({(i * 360) // len(expansions)}, 70%, 50%)'
            popup_html = f"""
                <div style='min-width: 200px'>
                    <h4>{group_name}</h4>
                    <p><b>Region:</b> {exp['region']}</p>
                    <p><b>Year:</b> {exp['year']}</p>
                    <p><b>Attacks:</b> {exp['attacks']}</p>
                    <p><b>Expansion:</b> {i + 1} of {len(expansions)}</p>
                    <p><b>Total Regions:</b> {region_count}</p>
                    <p><b>Years Active:</b> {expansion_years}</p>
                </div>
            """

            folium.CircleMarker(
                location=[lat, lon],
                radius=10,
                color=color,
                fill=True,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{group_name} - {exp['year']}",
            ).add_to(fg)

            valid_coords.append([lat, lon])

            # Draw lines connecting consecutive valid points
            if len(valid_coords) > 1:
                # Get previous valid coordinates
                prev_lat, prev_lon = valid_coords[-2]
                # Draw line
                folium.PolyLine(
                    locations=[[prev_lat, prev_lon], [lat, lon]],
                    color=color,
                    weight=2,
                    opacity=0.8,
                    tooltip=f"{expansions[i - 1]['year']} → {exp['year']}"
                ).add_to(fg)

        fg.add_to(m)

    # Add layer control
    folium.LayerControl().add_to(m)

    # Add summary information
    summary_html = f"""
    <div style='position: fixed; 
                bottom: 50px; 
                left: 50px; 
                z-index: 1000;
                background-color: white;
                padding: 10px;
                border: 2px solid #ccc;
                border-radius: 5px;'>
        <h4>Group Expansion Analysis</h4>
        <p>Number of expanding groups: {len(results)}</p>
        <p>Toggle groups using the layer control ↗</p>
        <ul style='list-style-type: none; padding-left: 0;'>
            <li>🔴 Marker = Activity start in region</li>
            <li>➡️ Line = Expansion direction</li>
            <li>🎨 Color indicates chronological order</li>
        </ul>
    </div>
    """
    m.get_root().html.add_child(folium.Element(summary_html))

    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf
# 13
def groups_coparticipation_service(connections):
    df = pd.DataFrame(connections, columns=['groups', 'count'])
    df[['group1', 'group2']] = pd.DataFrame(df['groups'].tolist(), index=df.index)
    df = df.sort_values('count', ascending=False).head(15)
    df['label'] = df['group1'] + '\n & \n' + df['group2']
    plt.figure(figsize=(15, 8))
    plt.bar(df['label'], df['count'])
    plt.title('Top 15 Group Co-participation in Attacks')
    plt.xlabel('Group Pairs')
    plt.ylabel('Number of Shared Attacks')
    plt.xticks(rotation=45, ha='right')
    for i, v in enumerate(df['count']):
        plt.text(i, v + 0.5, str(v), ha='center')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf
# 14
def common_attack_strategies_service(results):
    m = create_map()
    location_data = {}
    for result in results:
        key = (result['region'], result['country'])
        if key not in location_data:
            location_data[key] = {
                'attack_types': {},
                'location': result['region'] + (f", {result['country']}" if result['country'] else '')
            }
        attack_type = result['attack_type']
        location_data[key]['attack_types'][attack_type] = {
            'num_groups': result['num_groups'],
            'total_attacks': result['total_attacks'],
            'groups': result['groups']
        }
    valid_locations = 0
    skipped_locations = []
    for (region, country), data in location_data.items():
        location = get_locations_for_common_attacks(region, country)
        valid_coords = (
                location and
                location.latitude is not None and
                location.longitude is not None and
                isinstance(location.latitude, (int, float)) and
                isinstance(location.longitude, (int, float)) and
                not isnan(location.latitude) and
                not isnan(location.longitude)
        )
        if valid_coords:
            valid_locations += 1
            main_attack_type = max(
                data['attack_types'].items(),
                key=lambda x: (x[1]['num_groups'], x[1]['total_attacks'])
            )
            popup_content = f"""
                <div style='min-width: 300px'>
                    <h4>{data['location']}</h4>
                    <p><b>Most Common Strategy:</b> {main_attack_type[0]}</p>
                    <p><b>Groups Using This Strategy:</b> {main_attack_type[1]['num_groups']}</p>
                    <p><b>Total Attacks:</b> {main_attack_type[1]['total_attacks']}</p>
                    <hr>
                    <p><b>Groups:</b></p>
                    <ul>
                        {''.join(f'<li>{group}</li>' for group in main_attack_type[1]['groups'])}
                    </ul>
                    <hr>
                    <p><b>Other Common Strategies:</b></p>
                    <ul>
            """

            other_attacks = sorted(
                data['attack_types'].items(),
                key=lambda x: (x[1]['num_groups'], x[1]['total_attacks']),
                reverse=True
            )[1:4]

            for attack_type, attack_data in other_attacks:
                popup_content += f"""
                    <li>{attack_type} 
                        ({attack_data['num_groups']} groups, 
                        {attack_data['total_attacks']} attacks)</li>
                """

            popup_content += "</ul></div>"

            radius = min(20, main_attack_type[1]['num_groups'] * 3)

            try:
                folium.CircleMarker(
                    location=[float(location.latitude), float(location.longitude)],
                    radius=radius,
                    color='red',
                    fill=True,
                    popup=folium.Popup(popup_content, max_width=400),
                    tooltip=f"{data['location']}: {main_attack_type[1]['num_groups']} groups using {main_attack_type[0]}"
                ).add_to(m)
            except (ValueError, TypeError) as e:
                print(f"Error creating marker for {data['location']}: {str(e)}")
                skipped_locations.append(f"{data['location']} (invalid coordinates)")
        else:
            skipped_locations.append(data['location'])

    summary_html = f"""
    <div style='position: fixed; 
                bottom: 50px; 
                left: 50px; 
                z-index: 1000;
                background-color: white;
                padding: 10px;
                border: 2px solid #ccc;
                border-radius: 5px;'>
        <h4>Common Attack Strategies Analysis</h4>
        <p>Displayed Locations: {valid_locations} / {len(location_data)}</p>
        <p>🔴 Marker size indicates number of groups</p>
        <p>Click markers for detailed information</p>
    </div>
    """

    if skipped_locations:
        summary_html = summary_html.replace('</div>', f"""
        <p style='font-size: 0.8em; color: #666;'>
            Unable to map {len(skipped_locations)} location(s)
        </p>
        </div>
        """)

    m.get_root().html.add_child(folium.Element(summary_html))

    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf
# 16
def intergroup_activity_service(results, region_filter=None, country_filter=None):
    m = create_map()
    max_groups = max(result.unique_groups for result in results) if results else 1
    for result in results:
        if result.lat and result.lon:
            radius = min(30, (result.unique_groups / max_groups) * 30)
            events_per_group = result.total_events / result.unique_groups
            color = 'red' if events_per_group > 10 else 'orange' if events_per_group > 5 else 'yellow'
            group_list = sorted(group.strip(' "') for group in result.group_list)
            popup_html = f"""
                <div style='min-width: 300px'>
                    <h4>{result.region}, {result.country}</h4>
                    <p><b>Unique Groups:</b> {result.unique_groups}</p>
                    <p><b>Total Events:</b> {result.total_events}</p>
                    <p><b>Events per Group:</b> {events_per_group:.1f}</p>
                    <hr>
                    <p><b>Active Groups:</b></p>
                    <div style='max-height: 200px; overflow-y: auto;'>
                        <ul>
                            {''.join(f'<li>{group}</li>' for group in group_list)}
                        </ul>
                    </div>
                </div>
            """
            folium.CircleMarker(
                location=[result.lat, result.lon],
                radius=radius,
                color=color,
                fill=True,
                popup=folium.Popup(popup_html, max_width=400),
                tooltip=f"{result.region}, {result.country}: {result.unique_groups} groups"
            ).add_to(m)
    legend_html = """
    <div style="position: fixed; 
                bottom: 50px; 
                right: 50px; 
                width: 150px;
                height: 90px; 
                z-index: 1000;
                background-color: white;
                padding: 10px;
                border: 2px solid #ccc;">
        <p style="margin-bottom: 5px;"><b>Events per Group:</b></p>
        <p style="margin: 2px;">
            <span style="color: red;">●</span> > 10 events</p>
        <p style="margin: 2px;">
            <span style="color: orange;">●</span> 5-10 events</p>
        <p style="margin: 2px;">
            <span style="color: yellow;">●</span> < 5 events</p>
    </div>
    """
    summary_html = f"""
    <div style='position: fixed; 
                bottom: 50px; 
                left: 50px; 
                z-index: 1000;
                background-color: white;
                padding: 10px;
                border: 2px solid #ccc;
                border-radius: 5px;'>
        <h4>Inter-group Activity Analysis</h4>
        <p>Total Areas: {len(results)}</p>
        <p>⭕ Marker size = Number of unique groups</p>
        <p>🎨 Color = Events per group ratio</p>
        {f'<p>Filter: {region_filter or country_filter}</p>' if region_filter or country_filter else ''}
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    m.get_root().html.add_child(folium.Element(summary_html))
    buf = io.BytesIO()
    m.save(buf, close_file=False)
    return buf