import pandas as pd
from datetime import datetime
from sqlalchemy import func, case, desc, String, distinct, text
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
def casualties_by_region_repo(top_n):
    with session_maker() as session:
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
            func.avg(Location.latitude).label("lat"),
            func.avg(Location.longitude).label("lon")
        ).join(
            Location, Location.region_id == Region.id
        ).join(
            Event, Event.location_id == Location.id
        ).join(
            Casualties, Event.casualties_id == Casualties.id
        ).group_by(
            Region.name
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
def terror_heatmap_repo(time_period,region_filter):
    with session_maker() as session:
        query = session.query(
            Location.latitude,
            Location.longitude,
            Event.year,
            Event.month,
            Region.name.label('region')
        ).join(Event, Event.location_id == Location.id
               ).join(Region, Region.id == Location.region_id
                      ).filter(
            Location.latitude.isnot(None),
            Location.longitude.isnot(None)
        )
        current_year = datetime.now().year
        if time_period == 'month':
            current_month = datetime.now().month
            query = query.filter(
                Event.year == current_year,
                Event.month == current_month
            )
        elif time_period == '3_years':
            query = query.filter(Event.year >= current_year - 3)
        elif time_period == '5_years':
            query = query.filter(Event.year >= current_year - 5)
        if region_filter:
            query = query.filter(Region.name == region_filter)
        return query.all(), current_year
# 8
def active_groups_heatmap_repo(region_filter):
    with session_maker() as session:
        if region_filter:
            query = session.query(
                TerroristGroup.group_name,
                func.count(Event.id).label('attack_count'),
                func.avg(Location.latitude).label('avg_lat'),
                func.avg(Location.longitude).label('avg_lon')
            ).join(
                Event, Event.group_id == TerroristGroup.id
            ).join(
                Location, Location.id == Event.location_id
            ).join(
                Region, Region.id == Location.region_id
            ).filter(
                Region.name == region_filter
            ).group_by(
                TerroristGroup.group_name
            ).order_by(
                desc('attack_count')
            ).limit(5)
        else:
            rank_subquery = session.query(
                TerroristGroup.group_name,
                Region.name.label('region_name'),
                func.count(Event.id).label('attack_count'),
                func.avg(Location.latitude).label('avg_lat'),
                func.avg(Location.longitude).label('avg_lon'),
                func.row_number().over(
                    partition_by=Region.name,
                    order_by=desc(func.count(Event.id))
                ).label('rank')
            ).join(
                Event, Event.group_id == TerroristGroup.id
            ).join(
                Location, Location.id == Event.location_id
            ).join(
                Region, Region.id == Location.region_id
            ).group_by(
                TerroristGroup.group_name,
                Region.name
            ).subquery()
            query = session.query(
                rank_subquery.c.group_name,
                rank_subquery.c.region_name,
                rank_subquery.c.attack_count,
                rank_subquery.c.avg_lat,
                rank_subquery.c.avg_lon
            ).filter(
                rank_subquery.c.rank <= 5
            ).order_by(
                rank_subquery.c.region_name,
                rank_subquery.c.attack_count.desc()
            )
        return query.all()
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
def groups_coparticipation_repo():
    with session_maker() as session:
        base_data = session.query(
            Event.id.label('event_id'),
            Event.year,
            Event.month,
            Event.day,
            Event.summary,
            TerroristGroup.group_name
        ).join(
            TerroristGroup, Event.group_id == TerroristGroup.id
        ).filter(
            TerroristGroup.group_name != 'Unknown'
        ).all()
        event_groups = {}
        for event_id, year, month, day, summary, group_name in base_data:
            event_key = (year, month, day)
            if event_key not in event_groups:
                event_groups[event_key] = {
                    'groups': set(),
                    'event_ids': set(),
                    'summaries': set()
                }
            event_groups[event_key]['groups'].add(group_name)
            event_groups[event_key]['event_ids'].add(event_id)
            if summary:
                event_groups[event_key]['summaries'].add(summary)
        group_connections = {}
        for data in event_groups.values():
            groups = list(data['groups'])
            if len(groups) > 1:
                for i in range(len(groups)):
                    for j in range(i + 1, len(groups)):
                        group1, group2 = sorted([groups[i], groups[j]])
                        key = (group1, group2)
                        if key not in group_connections:
                            group_connections[key] = 0
                        group_connections[key] += 1
        return list(group_connections.items())
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
        if region_filter:
            query = query.filter(Region.name == region_filter)
        if country_filter:
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
def get_locations_for_common_attacks(region,country):
    with session_maker() as session:
        return session.query(
            func.avg(Location.latitude).label('lat'),
            func.avg(Location.longitude).label('lon')
        ).join(
            Region, Location.region_id == Region.id
        ).join(
            Country, Location.country_id == Country.id
        ).filter(
            Region.name == region,
            Country.name == country
        ).first()
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
