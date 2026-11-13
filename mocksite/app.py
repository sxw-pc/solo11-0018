"""Local Flask mock site with ETag/Last-Modified support."""

from __future__ import annotations

import hashlib
from datetime import timezone
from pathlib import Path

from flask import Flask, Response, jsonify, make_response, render_template, request

from .scenarios import (
    MAX_STEP, PAGE_PATHS, ScenarioState, context_for, last_modified_for,
)


def create_app(state: ScenarioState | None = None) -> Flask:
    app = Flask(__name__, template_folder=str(Path(__file__).parent / "pages"))
    scenario = state or ScenarioState()
    app.extensions["pagewatch_scenario"] = scenario

    def conditional(page: str, template: str) -> Response:
        step = scenario.get_step()
        html = render_template(template, step=step,
                               **context_for(page, step, scenario.seed))
        digest = hashlib.sha256(html.encode("utf-8")).hexdigest()
        etag = f'"{digest}"'
        modified = last_modified_for(page, step)
        if_none_match = request.headers.get("If-None-Match", "")
        if if_none_match and (etag in if_none_match or digest in if_none_match):
            return _not_modified(etag, modified, step)
        if_modified_since = request.if_modified_since
        if if_modified_since is not None:
            if if_modified_since.tzinfo is None:
                if_modified_since = if_modified_since.replace(tzinfo=timezone.utc)
            if modified <= if_modified_since:
                return _not_modified(etag, modified, step)
        response = make_response(html)
        response.set_etag(digest)
        response.last_modified = modified
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Pagewatch-Scenario"] = str(step)
        return response

    def _not_modified(etag: str, modified, step: int) -> Response:
        response = make_response("", 304)
        response.headers["ETag"] = etag
        response.last_modified = modified
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Pagewatch-Scenario"] = str(step)
        return response

    @app.get("/")
    def index() -> Response:
        step = scenario.get_step()
        links = "".join(f'<li><a href="{path}">{name}</a></li>'
                        for name, path in PAGE_PATHS.items())
        document = (f'<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
                    f"<title>pagewatch mock site</title></head><body>"
                    f"<h1>本地模拟站点</h1><p>scenario step: {step}</p>"
                    f"<ul>{links}</ul></body></html>")
        return Response(document, mimetype="text/html; charset=utf-8")

    @app.get("/announcements")
    def announcements() -> Response:
        return conditional("announcements", "announcements.html")

    @app.get("/docs")
    def docs() -> Response:
        return conditional("docs", "docs.html")

    @app.get("/pricing")
    def pricing() -> Response:
        return conditional("pricing", "pricing.html")

    @app.get("/noisy")
    def noisy() -> Response:
        return conditional("noisy", "noisy.html")

    @app.get("/listings")
    def listings() -> Response:
        return conditional("listings", "listings.html")

    @app.get("/healthz")
    def healthz() -> Response:
        return Response("ok\n", mimetype="text/plain; charset=utf-8")

    @app.post("/__control/step/<int:step>")
    def set_step(step: int) -> tuple[Response, int] | Response:
        if not 0 <= step <= MAX_STEP:
            return jsonify({"error": f"step must be in 0..{MAX_STEP}"}), 400
        scenario.set_step(step)
        return jsonify({"step": scenario.get_step()})

    @app.get("/__control/state")
    def scenario_state() -> Response:
        return jsonify({"step": scenario.get_step(), "seed": scenario.seed})

    return app


app = create_app()
