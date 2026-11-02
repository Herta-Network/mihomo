#!/usr/bin/env python3
"""Compare two complete proxy chains with exact-payload checks and alternating order."""
import argparse
import json
import os
from pathlib import Path
from run import execute


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('driver', 'base', 'candidate', 'output'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--trials', type=int, default=5)
    parser.add_argument('--rounds', type=int, default=3000)
    parser.add_argument('--paths', default='direct,vmess,vmess-xudp,vless,ss-udp,ss-uot,snell3,snell4,snell5,snell6,snell6-unshaped,snell6-unsafe-raw,anytls-uot,mux-smux,mux-h2mux,mux-yamux,hysteria2,tuic4,tuic5')
    parser.add_argument('--tcp-bytes', type=int, default=256 * 1024 * 1024)
    args = parser.parse_args()
    if args.trials < 1 or args.rounds < 1 or args.tcp_bytes < 65536 or args.tcp_bytes % 65536:
        parser.error('positive trials/rounds and a positive multiple of 65536 TCP bytes required')
    cases = [('udp', 64, 1, 1, args.rounds), ('udp', 1200, 1, 1, args.rounds),
             ('udp', 1200, 1, 4, args.rounds), ('udp', 1200, 4, 1, args.rounds)]
    environment = os.environ.copy()
    environment['GOMAXPROCS'] = '2'
    rows = []
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    for trial in range(1, args.trials + 1):
        for path in args.paths.split(','):
            path_cases = list(cases)
            # ss-udp also supports TCP, but shares its TCP carrier with ss-uot.
            if path != 'ss-udp':
                path_cases.append(('tcp', 65536, 1, 1, args.tcp_bytes // 65536))
            for mode, size, connections, batch, rounds in path_cases:
                case = f'{path}/{mode}/{size}/c{connections}/b{batch}'
                for side in (('base', 'candidate') if trial % 2 else ('candidate', 'base')):
                    command = [args.driver, '-binary', getattr(args, side), '-path', path,
                               '-mode', mode, '-size', str(size), '-connections', str(connections),
                               '-batch', str(batch), '-rounds', str(rounds)]
                    row = execute(command, environment)
                    try:
                        row['data'] = json.loads(row['output'])
                        del row['output']
                    except json.JSONDecodeError:
                        row['data'] = {'error': 'driver did not return JSON'}
                    row.update(trial=trial, case=case, side=side)
                    rows.append(row)
                    destination.write_text(json.dumps(rows, indent=2) + '\n')
                    print(trial, case, side, row['exit_code'], row['data'].get('error', ''), flush=True)


if __name__ == '__main__':
    main()
