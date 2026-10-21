"""Stateless keyed variates, stable across batches, retries and row order."""
import hashlib
import math


def uniform(seed, entity_id, namespace):
    digest = hashlib.blake2b(f'{seed}:{entity_id}:{namespace}'.encode(), digest_size=8).digest()
    return (int.from_bytes(digest, 'big') >> 11) / 2**53


def integer(cfg, key, namespace, count):
    return min(count - 1, int(uniform(cfg.seed, key, namespace) * count))


def normal(cfg, key, namespace):
    return math.sqrt(-2 * math.log(max(1e-15, uniform(cfg.seed, key, namespace+'a')))) * math.cos(2 * math.pi * uniform(cfg.seed, key, namespace+'b'))
