from itertools import combinations
from typing import Optional, List, Tuple, Dict, Set
import pandas as pd
from datetime import datetime
from sqlalchemy import func, case, desc, String, distinct, text, and_, Float, cast, literal
from toolz import pipe
from app.db.psql.database import session_maker
from app.db.psql.models import AttackType, Casualties, Event, Region, Location, TerroristGroup, TargetType, Country

# 1
def deadliest_attacks_repo(top_n):
    with session_maker() as session:
        query = session.query(
            AttackType.name.label("attack_type"),
            func.sum(
                case(
                    (Casualties.killed.isnot(None), Casualties.killed * 2),
                    else_=0
                ) +
                case(
                    (Casualties.wounded.isnot(None), Casualties.wounded),
                    else_=0
                )
            ).label("casualty_score")
        ).join(
            Event, Event.attack_type_id == AttackType.id
        ).join(
            Casualties, Event.casualties_id == Casualties.id
        ).group_by(
            AttackType.name
        ).order_by(
            desc("casualty_score")
        )
        if top_n:
            query = query.limit(top_n)
        return query.all()
# 2
def casualties_by_region_repo(top_n: Optional[int]) -> List[Tuple]:
    with session_maker() as session:
        valid_locations = session.query(
            Location.region_id,
            func.avg(Location.latitude).label("lat"),
            func.avg(Location.longitude).label("lon")
        ).filter(
            and_(
                Location.latitude.isnot(None),
                Location.longitude.isnot(None),
                Location.latitude != 0,
                Location.longitude != 0,
                Location.latitude.between(-90, 90),
                Location.longitude.between(-180, 180)
            )
        ).group_by(
            Location.region_id
        ).subquery()
        query = session.query(
            Region.name.label("region"),
            func.count(Event.id).label("event_count"),
            func.sum(
                case(
                    (Casualties.killed.isnot(None), Casualties.killed * 2),
                    else_=0
                ) +
                case(
                    (Casualties.wounded.isnot(None), Casualties.wounded),
                    else_=0
                )
            ).label("casualty_score"),
            valid_locations.c.lat,
            valid_locations.c.lon
        ).join(
            valid_locations, valid_locations.c.region_id == Region.id
        ).join(
            Location, Location.region_id == Region.id
        ).join(
            Event, Event.location_id == Location.id
        ).join(
            Casualties, Event.casualties_id == Casualties.id
        ).group_by(
            Region.name,
            valid_locations.c.lat,
            valid_locations.c.lon
        ).having(
            func.count(Event.id) > 0
        )

        if top_n:
            query = query.order_by(desc("casualty_score")).limit(top_n)

        return query.all()
# 3
def top_casualty_groups_repo():
    with session_maker() as session:
        return session.query(
            TerroristGroup.group_name,
            func.sum(Casualties.killed * 2 + Casualties.wounded).label("total_casualties"),
            func.min(Event.year).label("start_year"),
            func.max(Event.year).label("end_year"),
            func.count(Event.id).label("num_attacks")
        ).join(Event, Event.group_id == TerroristGroup.id
               ).join(Casualties, Event.casualties_id == Casualties.id
                      ).group_by(TerroristGroup.group_name
                                 ).order_by(desc("total_casualties")
                                            ).limit(5).all()
# 4
def attack_target_correlation_repo():
    with session_maker() as session:
        return session.query(
            AttackType.name,
            TargetType.name,
            func.count(Event.id).label("event_count")
        ).join(Event, Event.attack_type_id == AttackType.id
               ).join(TargetType, Event.target_type_id == TargetType.id
                      ).group_by(AttackType.name, TargetType.name).all()
# 5
def attack_trends_repo(year):
    with session_maker() as session:
        annual_trends = session.query(
            Event.year.label('year'),
            func.count(Event.id).label('attack_count')
        ).filter(Event.year.isnot(None)
                 ).group_by(Event.year).order_by(Event.year).all()
        monthly_trends = session.query(
            Event.month.label('month'),
            func.count(Event.id).label('attack_count')
        ).filter(
            Event.year == year,
            Event.month.isnot(None)
        ).group_by(Event.month).order_by(Event.month).all()
        return annual_trends, monthly_trends
# 6
def attack_change_by_region_repo():
    with session_maker() as session:
        attacks_by_region_year = session.query(
            Region.name.label('region'),
            Event.year.label('year'),
            func.count(Event.id).label('attack_count')
        ).join(Location, Location.region_id == Region.id
               ).join(Event, Event.location_id == Location.id
                      ).filter(Event.year.isnot(None)
                      ).group_by('region', Event.year).subquery()

        region_changes = session.query(
            attacks_by_region_year.c.region,
            attacks_by_region_year.c.year.label('current_year'),
            attacks_by_region_year.c.attack_count.label('current_attacks'),
            func.lag(attacks_by_region_year.c.attack_count).over(
                partition_by=[attacks_by_region_year.c.region],
                order_by=attacks_by_region_year.c.year
            ).label('previous_attacks'),
            func.lag(attacks_by_region_year.c.year).over(
                partition_by=[attacks_by_region_year.c.region],
                order_by=attacks_by_region_year.c.year
            ).label('previous_year')
        ).order_by(attacks_by_region_year.c.region, attacks_by_region_year.c.year)
        df = pd.read_sql(region_changes.statement, session.bind)
        return df
# 7
def terror_heatmap_repo(time_period, region_filter):
    with session_maker() as session:
        query = session.query(
            Location.latitude,
            Location.longitude,
            Event.year,
            Event.month,
            Region.name.label('region'),
            func.count(Event.id).label('event_count')
        ).join(
            Event, Event.location_id == Location.id
        ).join(
            Region, Region.id == Location.region_id
        ).filter(
            Location.latitude.isnot(None),
            Location.longitude.isnot(None),
            cast(Location.latitude, Float).isnot(None),
            cast(Location.longitude, Float).isnot(None)
        )
        current_year = 2017
        if time_period == 'month':
            current_month = datetime.now().month
            query = query.filter(
                Event.year == current_year,
                Event.month == current_month
            )
        elif time_period == 'year':
            query = query.filter(Event.year == current_year)
        elif time_period == '3_years':
            query = query.filter(Event.year >= current_year - 3)
        elif time_period == '5_years':
            query = query.filter(Event.year >= current_year - 5)
        if region_filter:
            query = query.filter(Region.name == region_filter)
        query = query.group_by(
            Location.latitude,
            Location.longitude,
            Event.year,
            Event.month,
            Region.name
        )
        results = query.all()
        if results:
            sample = results[0]
        return results, current_year
# 8
def active_groups_heatmap_repo(region_filter):
    with session_maker() as session:
        def get_region_center(region_name):
            return session.query(
                func.avg(case(
                    (Location.latitude.between(-90, 90), Location.latitude)
                )).label('avg_lat'),
                func.avg(case(
                    (Location.longitude.between(-180, 180), Location.longitude)
                )).label('avg_lon')
            ).join(
                Region, Location.region_id == Region.id
            ).filter(
                Region.name == region_name,
                Location.latitude != 0,
                Location.longitude != 0
            ).first()

        if region_filter:
            coords = get_region_center(region_filter)
            return list(session.query(
                TerroristGroup.group_name,
                func.count(Event.id).label('attack_count'),
                literal(coords.avg_lat).label('avg_lat'),
                literal(coords.avg_lon).label('avg_lon')
            ).join(
                Event
            ).join(
                Location
            ).join(
                Region
            ).filter(
                Region.name == region_filter
            ).group_by(
                TerroristGroup.group_name
            ).order_by(
                desc('attack_count')
            ).limit(5))
        else:
            regions = session.query(Region.name).all()
            results = []

            for region in regions:
                coords = get_region_center(region.name)
                if coords and coords.avg_lat and coords.avg_lon:
                    top_groups = session.query(
                        TerroristGroup.group_name,
                        func.count(Event.id).label('attack_count')
                    ).join(
                        Event
                    ).join(
                        Location
                    ).join(
                        Region
                    ).filter(
                        Region.name == region.name
                    ).group_by(
                        TerroristGroup.group_name
                    ).order_by(
                        desc('attack_count')
                    ).limit(5).all()

                    for group in top_groups:
                        results.append({
                            'region_name': region.name,
                            'group_name': group.group_name,
                            'attack_count': group.attack_count,
                            'avg_lat': float(coords.avg_lat),
                            'avg_lon': float(coords.avg_lon)
                        })

            return results
# 9
def perpetrators_casualties_correlation_repo():
    with session_maker() as session:
        return session.query(
            Event.id,
            func.count(TerroristGroup.id).label('perpetrator_count'),
            func.sum(
                case(
                    (Casualties.killed.isnot(None), Casualties.killed * 2),
                    else_=0
                ) +
                case(
                    (Casualties.wounded.isnot(None), Casualties.wounded),
                    else_=0
                )
            ).label('total_casualties')
        ).join(TerroristGroup, Event.group_id == TerroristGroup.id
               ).join(Casualties, Event.casualties_id == Casualties.id
                      ).group_by(Event.id).all()
# 10
def events_casualties_correlation_repo(region_name):
    with session_maker() as session:
        query = session.query(
            Region.name.label('region'),
            func.count(Event.id).label('event_count'),
            func.sum(
                case(
                    (Casualties.killed.isnot(None), Casualties.killed * 2),
                    else_=0
                ) +
                case(
                    (Casualties.wounded.isnot(None), Casualties.wounded),
                    else_=0
                )
            ).label("total_casualties")
        ).join(Location, Location.region_id == Region.id
               ).join(Event, Event.location_id == Location.id
                      ).join(Casualties, Event.casualties_id == Casualties.id
                             )
        if region_name:
            query = query.filter(Region.name == region_name)
        return query.group_by(Region.name).all()
# 11
def groups_common_goals_repo(region_filter=None, country_filter=None):
    with session_maker() as session:
        query = session.query(
            TerroristGroup.group_name,
            TargetType.name.label('target_type'),
            Region.name.label('region'),
            Country.name.label('country'),
            func.count(Event.id).label('attack_count'),
            func.avg(Location.latitude).label('lat'),
            func.avg(Location.longitude).label('lon')
        ).join(
            Event, Event.group_id == TerroristGroup.id
        ).join(
            TargetType, Event.target_type_id == TargetType.id
        ).join(
            Location, Event.location_id == Location.id
        ).join(
            Region, Location.region_id == Region.id
        ).join(
            Country, Location.country_id == Country.id
        )
        if region_filter:
            query = query.filter(Region.name == region_filter)
        if country_filter:
            query = query.filter(Country.name == country_filter)
        query = query.group_by(
            TerroristGroup.group_name,
            TargetType.name,
            Region.name,
            Country.name,
            Location.latitude,
            Location.longitude
        ).having(
            func.count(Event.id) > 0
        ).order_by(
            desc('attack_count')
        )
        return query.all()
# 12
def group_activity_expansion_repo():
    with session_maker() as session:
        first_appearance = session.query(
            TerroristGroup.group_name,
            Region.name.label('region_name'),
            func.min(Event.year).label('first_year'),
            func.avg(Location.latitude).label('lat'),
            func.avg(Location.longitude).label('lon'),
            func.count(Event.id).label('attack_count')
        ).join(
            Event, Event.group_id == TerroristGroup.id
        ).join(
            Location, Event.location_id == Location.id
        ).join(
            Region, Location.region_id == Region.id
        ).filter(
            Event.year.isnot(None)
        ).group_by(
            TerroristGroup.group_name,
            Region.name
        ).subquery()
        expansion_query = session.query(
            TerroristGroup.group_name,
            func.array_agg(
                func.json_build_object(
                    'region', first_appearance.c.region_name,
                    'year', first_appearance.c.first_year,
                    'lat', first_appearance.c.lat,
                    'lon', first_appearance.c.lon,
                    'attacks', first_appearance.c.attack_count
                ).cast(String)
            ).label('expansions'),
            func.count(distinct(first_appearance.c.region_name)).label('region_count')
        ).join(
            first_appearance, first_appearance.c.group_name == TerroristGroup.group_name
        ).group_by(
            TerroristGroup.group_name
        ).having(
            func.count(distinct(first_appearance.c.region_name)) > 1
        ).order_by(
            text('region_count DESC')
        ).limit(10)
        return expansion_query.all()
# 13
def groups_coparticipation_repo() -> List[Tuple[Tuple[str, str], int]]:
    def process_events(rows) -> Dict[Tuple[int, int, int], Set[str]]:
        events = {}
        for event_id, year, month, day, summary, group_name in rows:
            event_key = (year, month, day)
            if event_key not in events:
                events[event_key] = set()
            events[event_key].add(group_name)
        return events

    def count_connections(events: Dict) -> Dict[Tuple[str, str], int]:
        connections = {}
        for groups in events.values():
            if len(groups) > 1:
                for g1, g2 in combinations(groups, 2):
                    key = tuple(sorted([g1, g2]))
                    connections[key] = connections.get(key, 0) + 1
        return connections

    with session_maker() as session:
        return pipe(
            session.query(
                Event.id.label('event_id'),
                Event.year,
                Event.month,
                Event.day,
                Event.summary,
                TerroristGroup.group_name
            )
            .join(TerroristGroup, Event.group_id == TerroristGroup.id)
            .filter(TerroristGroup.group_name != 'Unknown')
            .all(),
            process_events,
            count_connections,
            dict.items,
            list
        )
# 14
def common_attack_strategies_repo(region_filter=None, country_filter=None):
    with session_maker() as session:
        query = session.query(
            Region.name.label('region'),
            Country.name.label('country'),
            AttackType.name.label('attack_type'),
            TerroristGroup.group_name,
            func.count(Event.id).label('attack_count')
        ).join(
            Event, Event.attack_type_id == AttackType.id
        ).join(
            TerroristGroup, Event.group_id == TerroristGroup.id
        ).join(
            Location, Event.location_id == Location.id
        ).join(
            Region, Location.region_id == Region.id
        ).join(
            Country, Location.country_id == Country.id
        ).filter(
            AttackType.name != 'Unknown'
        )

        # Apply filters if provided
        if region_filter and country_filter:
            query = query.filter(Region.name == region_filter, Country.name == country_filter)
        elif region_filter:
            query = query.filter(Region.name == region_filter)
        elif country_filter:
            query = query.filter(Country.name == country_filter)

        query = query.group_by(
            Region.name,
            Country.name,
            AttackType.name,
            TerroristGroup.group_name
        ).having(
            func.count(Event.id) > 0
        )

        results = query.all()
        area_strategies = {}

        for region, country, attack_type, group, count in results:
            area_key = (region, country)
            if area_key not in area_strategies:
                area_strategies[area_key] = {}
            if attack_type not in area_strategies[area_key]:
                area_strategies[area_key][attack_type] = {'groups': set(), 'total_attacks': 0}
            area_strategies[area_key][attack_type]['groups'].add(group)
            area_strategies[area_key][attack_type]['total_attacks'] += count

        formatted_data = []
        for (region, country), strategies in area_strategies.items():
            for attack_type, data in strategies.items():
                if len(data['groups']) > 1:
                    formatted_data.append({
                        'region': region,
                        'country': country,
                        'attack_type': attack_type,
                        'num_groups': len(data['groups']),
                        'total_attacks': data['total_attacks'],
                        'groups': list(data['groups'])
                    })

        return sorted(formatted_data,
                      key=lambda x: (x['num_groups'], x['total_attacks']),
                      reverse=True)
def get_locations_for_common_attacks(region, country):
    with session_maker() as session:
        # Try to get country-level location first if country is provided
        if country:
            location = session.query(Location).join(
                Country, Location.country_id == Country.id
            ).join(
                Region, Location.region_id == Region.id
            ).filter(
                Region.name == region,
                Country.name == country,
                Location.latitude.isnot(None),
                Location.longitude.isnot(None)
            ).first()

            if location:
                return location

        # If no country-level location found or no country provided, try region-level
        location = session.query(Location).join(
            Region, Location.region_id == Region.id
        ).join(
            Country, Location.country_id == Country.id
        ).filter(
            Region.name == region,
            Location.latitude.isnot(None),
            Location.longitude.isnot(None)
        ).order_by(
            Country.name
        ).first()

        return location
# 16
def intergroup_activity_repo(region_filter=None, country_filter=None):
    with session_maker() as session:
        query = session.query(
            Region.name.label('region'),
            Country.name.label('country'),
            func.avg(Location.latitude).label('lat'),
            func.avg(Location.longitude).label('lon'),
            func.count(distinct(TerroristGroup.id)).label('unique_groups'),
            func.count(Event.id).label('total_events'),
            func.array_agg(distinct(TerroristGroup.group_name)).label('group_list')
        ).join(
            Location, Location.region_id == Region.id
        ).join(
            Country, Location.country_id == Country.id
        ).join(
            Event, Event.location_id == Location.id
        ).join(
            TerroristGroup, Event.group_id == TerroristGroup.id
        )
        if region_filter:
            query = query.filter(Region.name == region_filter)
        if country_filter:
            query = query.filter(Country.name == country_filter)
        query = query.group_by(
            Region.name,
            Country.name
        ).having(
            func.count(distinct(TerroristGroup.id)) > 1
        ).order_by(
            desc('unique_groups')
        )
        return query.all()