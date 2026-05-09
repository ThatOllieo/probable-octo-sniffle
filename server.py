#!/usr/bin/env python3
"""
Scoreboard WebSocket server — Generic, Basketball, Futsal, and Volleyball modes.
Persists state to scores.json — survives restarts.

Usage: python server.py
Then open scoreboard.html in OBS (or a browser) and control.html in a browser.

─── General ──────────────────────────────────────────────────────────────────
  1 <name>           set team 1 name
  2 <name>           set team 2 name
  s1 <n>             set team 1 score
  s2 <n>             set team 2 score
  +1 [n]             add n (default 1) to team 1 score
  +2 [n]             add n (default 1) to team 2 score
  -1 [n]             subtract n from team 1 score
  -2 [n]             subtract n from team 2 score
  title <text>       set scoreboard title
  show               make scoreboard visible
  hide               hide scoreboard
  reset              reset scores, period, fouls, timeouts to mode defaults
  state              print current state
  help               show this help

─── Layout ───────────────────────────────────────────────────────────────────
  corner <pos>       position corner: tl / tr / bl / br
                     (top-left, top-right, bottom-left, bottom-right)

─── Element visibility ───────────────────────────────────────────────────────
  showperiod / hideperiod       toggle the period/quarter badge
  showfouls / hidefouls         toggle fouls display in stats bar
  showtimeouts / hidetimeouts   toggle timeouts display in stats bar

─── Sport mode ───────────────────────────────────────────────────────────────
  mode <sport>       switch mode: generic / basketball / futsal / volleyball
                     (resets score, period, fouls, timeouts; keeps team names)

─── Period / set ─────────────────────────────────────────────────────────────
  period <n>         set period number (quarter / half / set, by mode)
  np                 advance to next period

─── Basketball & futsal ──────────────────────────────────────────────────────
  +f1 [n]            add n fouls (default 1) to team 1
  +f2 [n]            add n fouls (default 1) to team 2
  f1 <n>             set team 1 fouls
  f2 <n>             set team 2 fouls
  resetfouls         reset both teams' fouls to 0
  -t1                use one timeout for team 1
  -t2                use one timeout for team 2
  t1 <n>             set team 1 timeouts remaining
  t2 <n>             set team 2 timeouts remaining

─── Basketball scoring shortcuts ─────────────────────────────────────────────
  ft1 / ft2          free throw  (+1) for team 1 / team 2
  2p1 / 2p2          field goal  (+2) for team 1 / team 2
  3p1 / 3p2          three-pointer (+3) for team 1 / team 2

─── Volleyball ───────────────────────────────────────────────────────────────
  winset 1           award current set to team 1, reset set scores, next set
  winset 2           award current set to team 2, reset set scores, next set
  sets1 <n>          set team 1's sets won
  sets2 <n>          set team 2's sets won
"""

import asyncio
import json
import os
import sys

try:
    import websockets
except ImportError:
    print("Missing dependency. Run:  pip install websockets")
    sys.exit(1)

SCORES_FILE = os.path.join(os.path.dirname(__file__), "scores.json")
HOST = "localhost"
PORT = 8765

MODE_DEFAULTS = {
    "generic":    {"timeouts": 0, "fouls": 0, "sets": 0},
    "basketball": {"timeouts": 5, "fouls": 0, "sets": 0},
    "futsal":     {"timeouts": 1, "fouls": 0, "sets": 0},
    "volleyball": {"timeouts": 0, "fouls": 0, "sets": 0},
}

DEFAULT_STATE = {
    "mode": "generic",
    "title": "Scoreboard",
    "corner": "br",
    "show_period": True,
    "show_fouls": True,
    "show_timeouts": True,
    "team1": {"name": "Team 1", "score": 0, "fouls": 0, "timeouts": 0, "sets": 0},
    "team2": {"name": "Team 2", "score": 0, "fouls": 0, "timeouts": 0, "sets": 0},
    "period": 1,
    "visible": True,
}

connected: set = set()


# ── persistence ──────────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(SCORES_FILE):
        try:
            with open(SCORES_FILE) as f:
                data = json.load(f)
            merged = json.loads(json.dumps(DEFAULT_STATE))
            merged["team1"] = {**DEFAULT_STATE["team1"], **data.get("team1", {})}
            merged["team2"] = {**DEFAULT_STATE["team2"], **data.get("team2", {})}
            merged["title"]         = data.get("title",         DEFAULT_STATE["title"])
            merged["visible"]       = data.get("visible",       DEFAULT_STATE["visible"])
            merged["mode"]          = data.get("mode",          DEFAULT_STATE["mode"])
            merged["period"]        = data.get("period",        DEFAULT_STATE["period"])
            merged["corner"]        = data.get("corner",        DEFAULT_STATE["corner"])
            merged["show_period"]   = data.get("show_period",   DEFAULT_STATE["show_period"])
            merged["show_fouls"]    = data.get("show_fouls",    DEFAULT_STATE["show_fouls"])
            merged["show_timeouts"] = data.get("show_timeouts", DEFAULT_STATE["show_timeouts"])
            return merged
        except Exception as e:
            print(f"[warn] Could not load {SCORES_FILE}: {e}. Starting fresh.")
    return json.loads(json.dumps(DEFAULT_STATE))


def save_state(s: dict) -> None:
    try:
        with open(SCORES_FILE, "w") as f:
            json.dump(s, f, indent=2)
    except Exception as e:
        print(f"[warn] Could not save state: {e}")


# ── websocket ─────────────────────────────────────────────────────────────────

async def broadcast(s: dict) -> None:
    if not connected:
        return
    msg = json.dumps(s)
    results = await asyncio.gather(
        *[ws.send(msg) for ws in list(connected)],
        return_exceptions=True,
    )
    for ws, result in zip(list(connected), results):
        if isinstance(result, Exception):
            connected.discard(ws)


async def handler(websocket) -> None:
    connected.add(websocket)
    try:
        await websocket.send(json.dumps(state))
        async for message in websocket:
            if process_command(message):
                save_state(state)
                await broadcast(state)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected.discard(websocket)


# ── helpers ───────────────────────────────────────────────────────────────────

def clamp(n: int) -> int:
    return max(0, n)


def period_label(mode: str, period: int) -> str:
    if mode == "basketball":
        if period <= 4:
            return f"Q{period}"
        return "OT" if period == 5 else f"OT{period - 4}"
    if mode == "futsal":
        if period == 1: return "1H"
        if period == 2: return "2H"
        if period == 3: return "ET"
        return f"ET{period - 2}"
    if mode == "volleyball":
        return f"Set {period}"
    return f"P{period}"


def print_state(s: dict) -> None:
    t1, t2 = s["team1"], s["team2"]
    mode = s.get("mode", "generic")
    per  = period_label(mode, s.get("period", 1))
    corner = s.get("corner", "br").upper()
    sp = "ON" if s.get("show_period", True) else "OFF"
    sf = "ON" if s.get("show_fouls", True) else "OFF"
    st = "ON" if s.get("show_timeouts", True) else "OFF"
    print(f"\n  Mode: {mode.upper()}   Period: {per}   Visible: {s['visible']}   Corner: {corner}")
    print(f"  Title: {s['title']}   Period badge: {sp}   Fouls: {sf}   Timeouts: {st}\n")
    if mode == "volleyball":
        print(f"  {'NAME':<22} {'SETS':>5} {'SCORE':>7}")
        print(f"  {t1['name']:<22} {t1.get('sets', 0):>5} {t1['score']:>7}")
        print(f"  {t2['name']:<22} {t2.get('sets', 0):>5} {t2['score']:>7}")
    elif mode in ("basketball", "futsal"):
        print(f"  {'NAME':<22} {'FOULS':>6} {'TO':>4} {'SCORE':>7}")
        print(f"  {t1['name']:<22} {t1.get('fouls', 0):>6} {t1.get('timeouts', 0):>4} {t1['score']:>7}")
        print(f"  {t2['name']:<22} {t2.get('fouls', 0):>6} {t2.get('timeouts', 0):>4} {t2['score']:>7}")
    else:
        print(f"  {t1['name']:<22} {t1['score']}")
        print(f"  {t2['name']:<22} {t2['score']}")
    print()


# ── command processing ────────────────────────────────────────────────────────

def process_command(raw: str) -> bool:
    """Execute a command string, mutating state in place. Returns True if state changed."""
    parts = raw.strip().split(None, 1)
    if not parts:
        return False

    cmd     = parts[0].lower()
    arg     = parts[1] if len(parts) > 1 else ""
    changed = True

    try:
        if cmd == "help":
            print(__doc__)
            changed = False

        elif cmd == "state":
            print_state(state)
            changed = False

        # ── team names ────────────────────────────────────────────────
        elif cmd == "1":
            if not arg: print("Usage: 1 <name>"); changed = False
            else: state["team1"]["name"] = arg
        elif cmd == "2":
            if not arg: print("Usage: 2 <name>"); changed = False
            else: state["team2"]["name"] = arg

        # ── scores ────────────────────────────────────────────────────
        elif cmd == "s1": state["team1"]["score"] = int(arg)
        elif cmd == "s2": state["team2"]["score"] = int(arg)
        elif cmd == "+1":
            n = int(arg) if arg else 1
            state["team1"]["score"] = clamp(state["team1"]["score"] + n)
        elif cmd == "+2":
            n = int(arg) if arg else 1
            state["team2"]["score"] = clamp(state["team2"]["score"] + n)
        elif cmd == "-1":
            n = int(arg) if arg else 1
            state["team1"]["score"] = clamp(state["team1"]["score"] - n)
        elif cmd == "-2":
            n = int(arg) if arg else 1
            state["team2"]["score"] = clamp(state["team2"]["score"] - n)

        # ── basketball scoring shortcuts ──────────────────────────────
        elif cmd == "ft1": state["team1"]["score"] = clamp(state["team1"]["score"] + 1)
        elif cmd == "ft2": state["team2"]["score"] = clamp(state["team2"]["score"] + 1)
        elif cmd == "2p1": state["team1"]["score"] = clamp(state["team1"]["score"] + 2)
        elif cmd == "2p2": state["team2"]["score"] = clamp(state["team2"]["score"] + 2)
        elif cmd == "3p1": state["team1"]["score"] = clamp(state["team1"]["score"] + 3)
        elif cmd == "3p2": state["team2"]["score"] = clamp(state["team2"]["score"] + 3)

        # ── mode ──────────────────────────────────────────────────────
        elif cmd == "mode":
            new_mode = arg.lower().strip()
            if new_mode not in MODE_DEFAULTS:
                print(f"Unknown mode. Choose: {', '.join(MODE_DEFAULTS)}")
                changed = False
            else:
                defs = MODE_DEFAULTS[new_mode]
                state["mode"]   = new_mode
                state["period"] = 1
                for t in ("team1", "team2"):
                    state[t]["score"]    = 0
                    state[t]["fouls"]    = defs["fouls"]
                    state[t]["timeouts"] = defs["timeouts"]
                    state[t]["sets"]     = defs["sets"]

        # ── period ────────────────────────────────────────────────────
        elif cmd in ("period", "quarter", "half", "set"):
            state["period"] = int(arg)
        elif cmd in ("np", "nextperiod"):
            state["period"] = state.get("period", 1) + 1

        # ── fouls ─────────────────────────────────────────────────────
        elif cmd == "+f1":
            n = int(arg) if arg else 1
            state["team1"]["fouls"] = clamp(state["team1"].get("fouls", 0) + n)
        elif cmd == "+f2":
            n = int(arg) if arg else 1
            state["team2"]["fouls"] = clamp(state["team2"].get("fouls", 0) + n)
        elif cmd == "f1": state["team1"]["fouls"] = int(arg)
        elif cmd == "f2": state["team2"]["fouls"] = int(arg)
        elif cmd == "resetfouls":
            state["team1"]["fouls"] = 0
            state["team2"]["fouls"] = 0

        # ── timeouts ──────────────────────────────────────────────────
        elif cmd == "-t1":
            state["team1"]["timeouts"] = clamp(state["team1"].get("timeouts", 0) - 1)
        elif cmd == "-t2":
            state["team2"]["timeouts"] = clamp(state["team2"].get("timeouts", 0) - 1)
        elif cmd == "t1": state["team1"]["timeouts"] = int(arg)
        elif cmd == "t2": state["team2"]["timeouts"] = int(arg)

        # ── volleyball sets ───────────────────────────────────────────
        elif cmd == "winset":
            team = int(arg)
            if team not in (1, 2):
                print("Usage: winset 1  or  winset 2"); changed = False
            else:
                state[f"team{team}"]["sets"] = state[f"team{team}"].get("sets", 0) + 1
                state["team1"]["score"] = 0
                state["team2"]["score"] = 0
                state["period"] = state.get("period", 1) + 1
        elif cmd == "sets1": state["team1"]["sets"] = int(arg)
        elif cmd == "sets2": state["team2"]["sets"] = int(arg)

        # ── corner / element visibility ───────────────────────────────
        elif cmd == "corner":
            if arg not in ("tl", "tr", "bl", "br"):
                print("Usage: corner <tl|tr|bl|br>"); changed = False
            else:
                state["corner"] = arg
        elif cmd == "showperiod":   state["show_period"] = True
        elif cmd == "hideperiod":   state["show_period"] = False
        elif cmd == "showfouls":    state["show_fouls"] = True
        elif cmd == "hidefouls":    state["show_fouls"] = False
        elif cmd == "showtimeouts": state["show_timeouts"] = True
        elif cmd == "hidetimeouts": state["show_timeouts"] = False

        # ── title / visibility / reset ────────────────────────────────
        elif cmd == "title":
            if not arg: print("Usage: title <text>"); changed = False
            else: state["title"] = arg
        elif cmd == "show":  state["visible"] = True
        elif cmd == "hide":  state["visible"] = False
        elif cmd == "reset":
            defs = MODE_DEFAULTS[state.get("mode", "generic")]
            state["period"] = 1
            for t in ("team1", "team2"):
                state[t]["score"]    = 0
                state[t]["fouls"]    = defs["fouls"]
                state[t]["timeouts"] = defs["timeouts"]
                state[t]["sets"]     = defs["sets"]

        else:
            print(f"Unknown command: {cmd!r}. Type 'help' for commands.")
            changed = False

    except ValueError:
        print(f"Invalid number: {arg!r}")
        changed = False

    return changed


# ── CLI ───────────────────────────────────────────────────────────────────────

async def cli(loop: asyncio.AbstractEventLoop) -> None:
    print(f"\nScoreboard server running on ws://{HOST}:{PORT}")
    print(f"State file: {SCORES_FILE}")
    print(f"Control panel: open control.html in a browser")
    print('Type "help" for commands.\n')
    print_state(state)

    while True:
        try:
            raw = await loop.run_in_executor(None, lambda: input("> "))
        except (EOFError, KeyboardInterrupt):
            print("\nShutting down.")
            break

        if process_command(raw):
            save_state(state)
            await broadcast(state)
            print_state(state)


# ── main ──────────────────────────────────────────────────────────────────────

state = load_state()


async def main() -> None:
    loop = asyncio.get_running_loop()
    async with websockets.serve(handler, HOST, PORT):
        await cli(loop)


if __name__ == "__main__":
    asyncio.run(main())
