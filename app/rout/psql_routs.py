from datetime import datetime
from flask import Blueprint, Response, request
from app.repository.psql_repository import deadliest_attacks_repo, casualties_by_region_repo, top_casualty_groups_repo, \
    attack_target_correlation_repo, attack_trends_repo, attack_change_by_region_repo, terror_heatmap_repo, \
    active_groups_heatmap_repo, perpetrators_casualties_correlation_repo, events_casualties_correlation_repo, \
    groups_common_goals_repo, group_activity_expansion_repo, groups_coparticipation_repo, common_attack_strategies_repo, \
    intergroup_activity_repo
from app.service.psql_service import top_casualty_groups_service, casualties_by_region_service, \
    deadliest_attacks_service, attack_target_correlation_service, attack_trends_service, \
    attack_change_by_region_service, terror_heatmap_service, active_groups_heatmap_service, \
    perpetrators_casualties_correlation_service, events_casualties_correlation_service, groups_common_goals_service, \
    group_activity_expansion_service, groups_coparticipation_service, common_attack_strategies_service, \
    intergroup_activity_service

stats_blueprint = Blueprint('stats', __name__)

#1
@stats_blueprint.route('/deadliest_attacks')
def deadliest_attacks():
    top_n = request.args.get('top_n', type=int, default=5)
    results = deadliest_attacks_repo(top_n)
    buf = deadliest_attacks_service(results)
    return Response(buf.getvalue(), mimetype='image/png')

#2
@stats_blueprint.route('/casualties_by_region')
def casualties_by_region():
    top_n = request.args.get('top_n', type=int)
    results = casualties_by_region_repo(top_n)
    buf = casualties_by_region_service(results)
    return Response(buf.getvalue(), mimetype='text/html')

#3
@stats_blueprint.route('/top_casualty_groups')
def top_casualty_groups():
    results = top_casualty_groups_repo()
    buf = top_casualty_groups_service(results)
    return Response(buf.getvalue(), mimetype='image/png')

#4
@stats_blueprint.route('/attack_target_correlation')
def attack_target_correlation():
    results = attack_target_correlation_repo()
    buf = attack_target_correlation_service(results)
    return Response(buf.getvalue(), mimetype='image/png')

#5
@stats_blueprint.route('/attack_trends')
def attack_trends():
    year = request.args.get('year', type=int, default=datetime.now().year)
    annual_trends, monthly_trends = attack_trends_repo(year)
    buf = attack_trends_service(annual_trends, monthly_trends,year)
    return Response(buf.getvalue(), mimetype='image/png')

#6
@stats_blueprint.route('/attack_change_by_region')
def attack_change_by_region():
    top_n = request.args.get('top_n', type=int, default=5)
    df = attack_change_by_region_repo()
    buf = attack_change_by_region_service(df, top_n)
    return Response(buf.getvalue(), mimetype='image/png')

#7
@stats_blueprint.route('/terror_heatmap')
def terror_heatmap():
    time_period = request.args.get('period', default='year', type=str)
    region_filter = request.args.get('region', type=str)
    locations, current_year = terror_heatmap_repo(time_period,region_filter)
    buf = terror_heatmap_service(locations, current_year, time_period, region_filter)
    return Response(buf.getvalue(), mimetype='text/html')

#8
@stats_blueprint.route('/active_groups_heatmap')
def active_groups_heatmap():
    region_filter = request.args.get('region', type=str)
    results = active_groups_heatmap_repo(region_filter)
    buf = active_groups_heatmap_service(results,region_filter)
    return Response(buf.getvalue(), mimetype='text/html')

#9
@stats_blueprint.route('/perpetrators_casualties_correlation')
def perpetrators_casualties_correlation():
    results = perpetrators_casualties_correlation_repo()
    buf = perpetrators_casualties_correlation_service(results)
    return Response(buf.getvalue(), mimetype='image/png')

#10
@stats_blueprint.route('/events_casualties_correlation')
def events_casualties_correlation():
    region_name = request.args.get('region', type=str)
    results = events_casualties_correlation_repo(region_name)
    buf = events_casualties_correlation_service(results, region_name)
    return Response(buf.getvalue(), mimetype='image/png')

# 11
@stats_blueprint.route('/groups_common_goals')
def groups_common_goals():
    region_filter = request.args.get('region', type=str)
    country_filter = request.args.get('country', type=str)
    results = groups_common_goals_repo(region_filter, country_filter)
    buf = groups_common_goals_service(results, region_filter, country_filter)
    return Response(buf.getvalue(), mimetype='text/html')

# 12
@stats_blueprint.route('/group_activity_expansion')
def group_activity_expansion():
    results = group_activity_expansion_repo()
    buf = group_activity_expansion_service(results)
    return Response(buf.getvalue(), mimetype='text/html')

# 13
@stats_blueprint.route('/groups_coparticipation')
def groups_coparticipation():
    connections = groups_coparticipation_repo()
    buf = groups_coparticipation_service(connections)
    return Response(buf.getvalue(), mimetype='image/png')

# 14
@stats_blueprint.route('/common_attack_strategies')
def common_attack_strategies():
    region_filter = request.args.get('region', type=str)
    country_filter = request.args.get('country', type=str)
    results = common_attack_strategies_repo(region_filter, country_filter)
    buf = common_attack_strategies_service(results)
    return Response(buf.getvalue(), mimetype='text/html')

# 16
@stats_blueprint.route('/intergroup_activity')
def intergroup_activity():
    region_filter = request.args.get('region', type=str)
    country_filter = request.args.get('country', type=str)
    results = intergroup_activity_repo(region_filter, country_filter)
    buf = intergroup_activity_service(results, region_filter, country_filter)
    return Response(buf.getvalue(), mimetype='text/html')
