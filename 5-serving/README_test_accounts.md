## Test accounts

The dashboard requires a sign-in. Account creation is deliberately not
implemented: three fixed test accounts are seeded by
`python 5-serving/seed_test_users.py` (run once, after the stores and the
backend are up).

| Username | Password | Supply chain modelled |
|---|---|---|
| `radar_electronics` | `chips2026` | Semiconductors / electronics — Asia-Pacific |
| `radar_pharma` | `vials2026` | Pharmaceuticals / biologics — Europe |
| `radar_agrifood` | `grain2026` | Agri-food commodities — Americas + Africa |

The three profiles are mutually exclusive: no territory and no keyword appears
in more than one account, and every supply-chain question carries at least 20
answers, so a briefing can be attributed to exactly one account on sight.

Inactive users are signed out after 15 minutes. These are test accounts with
short passwords and no password-change flow; the sign-in gates the dashboard
UI, not the backend API, and is not production authentication.
