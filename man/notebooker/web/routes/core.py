from flask import Blueprint, request, jsonify

from man.notebooker.serialization.serialization import get_serializer
from man.notebooker.utils.results import get_all_available_results_json
from man.notebooker.utils.templates import get_all_possible_templates as utils_get_all_possible_templates


core_bp = Blueprint('core_bp', __name__)


@core_bp.route("/core/user_profile")
def user_profile():
    user_roles = request.headers.get("X-Auth-Roles")
    username = request.headers.get("X-Auth-Username")
    return jsonify({"username": username, "roles": user_roles})


@core_bp.route("/core/get_all_available_results")
def all_available_results():
    limit = int(request.args.get('limit', 50))
    return jsonify(get_all_available_results_json(get_serializer(), limit))


@core_bp.route("/core/all_possible_templates")
def get_all_possible_templates():
    return jsonify(utils_get_all_possible_templates())
