"""Discord webhook delivery for every daemon that pages a human.

Discord's Cloudflare edge answers 403 to the default ``Python-urllib`` agent while
the webhook itself stays valid, so a poster that sends the default agent, or never
checks the status, reports success for alerts nobody receives. The webhook token
sits in the URL path, so no error raised here may carry the URL.
"""

import json
import urllib.error
import urllib.request

USER_AGENT = "materios-operator-kit/1.0 (+https://github.com/Flux-Point-Studios/materios-operator-kit)"


class DiscordError(Exception):
    """The webhook did not accept the post."""


def post_json(webhook_url: str, payload: dict, *, timeout: float = 10.0) -> None:
    try:
        request = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
    except urllib.error.HTTPError as e:
        raise DiscordError(f"webhook answered HTTP {e.code}") from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise DiscordError(f"webhook unreachable: {type(e).__name__}") from None
    if not 200 <= status < 300:
        raise DiscordError(f"webhook answered HTTP {status}")
