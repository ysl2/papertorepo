from __future__ import annotations

from collections import defaultdict

from papertorepo.core.records import PaperRepoLink, RepoObservation


FINAL_FOUND = "found"
FINAL_AMBIGUOUS = "ambiguous"
FINAL_NOT_FOUND = "not_found"


def build_final_links(arxiv_id: str, observations: list[RepoObservation]) -> list[dict]:
    grouped: dict[str, dict[str, set[str]]] = defaultdict(lambda: {"providers": set(), "surfaces": set()})
    for observation in observations:
        if observation.status != "found" or not observation.github_url:
            continue
        grouped[observation.github_url]["providers"].add(observation.provider)
        grouped[observation.github_url]["surfaces"].add(f"{observation.provider}:{observation.surface}")

    if not grouped:
        return []

    ranked_urls = sorted(
        grouped,
        key=lambda url: (
            -len(grouped[url]["providers"]),
            -len(grouped[url]["surfaces"]),
            url,
        ),
    )
    ambiguous = len(ranked_urls) > 1
    links: list[dict] = []
    for index, url in enumerate(ranked_urls):
        providers = grouped[url]["providers"]
        surfaces = grouped[url]["surfaces"]
        links.append(
            {
                "github_url": url,
                "status": FINAL_AMBIGUOUS if ambiguous else FINAL_FOUND,
                "providers": providers,
                "surfaces": surfaces,
                "provider_count": len(providers),
                "surface_count": len(surfaces),
                "is_primary": index == 0,
            }
        )
    return links


def parity_summary(observations: list[RepoObservation], final_links: list[PaperRepoLink]) -> dict[str, object]:
    by_surface: dict[str, str] = {}
    found_any = False
    for observation in observations:
        key = f"{observation.provider}:{observation.surface}"
        by_surface[key] = observation.status
        if observation.status == "found":
            found_any = True

    final_status = FINAL_NOT_FOUND
    if final_links:
        final_status = FINAL_AMBIGUOUS if any(link.status == FINAL_AMBIGUOUS for link in final_links) else FINAL_FOUND

    return {
        "found_any_provider_link": found_any,
        "surface_statuses": by_surface,
        "final_status": final_status,
        "link_count": len(final_links),
    }
