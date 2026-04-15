"""Persona assignment helpers.

Currently thin — the heavy lifting lives in events.py. This file exists to
make it easy to swap in richer persona logic (eg. Markov-chain transitions
between personas over time) without touching the event generator.
"""

from __future__ import annotations

import numpy as np

from .. import config


def assign_persona(rng: np.random.Generator) -> str:
    """Sample a persona id according to global share weights."""
    persona_ids = list(config.PERSONAS.keys())
    weights = np.array([config.PERSONAS[p]["share"] for p in persona_ids])
    weights /= weights.sum()
    return str(rng.choice(persona_ids, p=weights))
