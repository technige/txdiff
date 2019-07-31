
from re import compile
from sys import argv


# Start[master=-1,me=-1,time=2019-07-26 14:47:37.479+0000/1564152457479,lastCommittedTxWhenTransactionStarted=10180000,additionalHeaderLength=8,[0, 0, 0, 0, 0, 90, 25, -103],position=LogPosition{logVersion=93, byteOffset=7551435},checksum=-160721157253]
start = compile(r"Start\[(.*)\]\n")

# Commit[txId=10190000, 2019-07-26 18:52:51.910+0000/1564167171910]
commit = compile(r"Commit\[txId=(.*?), (.*?)\]\n")


tx_ids = set()


class Log:

    def __init__(self, name, data):
        self.name = name
        self.data = data
        self.tx_lines = {}

    def __len__(self):
        return len(self.data)

    def find_tx(self):
        start_line_no = None
        for line_no, line in enumerate(self.data):
            matched = start.match(line)
            if matched:
                start_line_no = line_no
            else:
                matched = commit.match(line)
                if matched:
                    tx_id = matched.group(1)
                    tx_id = int(tx_id)
                    if start_line_no is not None:
                        tx_ids.add(tx_id)
                        self.tx_lines[tx_id] = range(start_line_no, line_no + 1)


def main():
    files = {name: Log(name, open(name).readlines()) for name in argv[1:]}
    overlapping_ids = None
    for name, log in files.items():
        print("Scanning {} ({} lines)".format(name, len(log)))
        log.find_tx()
        if overlapping_ids is None:
            overlapping_ids = set(log.tx_lines.keys())
        else:
            overlapping_ids &= set(log.tx_lines.keys())
    first_tx_id = min(overlapping_ids)
    sorted_tx_ids = sorted(tx_id for tx_id in tx_ids if tx_id >= first_tx_id)
    for tx_id in sorted_tx_ids:
        print("Transaction {}".format(tx_id))
        found = 0
        sizes = set()
        for name, log in sorted(files.items()):
            try:
                span = log.tx_lines[tx_id]
            except KeyError:
                print("  File {}: not found".format(name))
            else:
                found += 1
                sizes.add(span.stop - span.start)
                print("  File {}: lines {}-{}".format(name, span.start, span.stop))
        if found != len(files) or len(sizes) > 1:
            print("MISMATCH!! (found={}, sizes={})".format(found, len(sizes)))
            for name, log in sorted(files.items()):
                print(name)
                for line_no in log.tx_lines[tx_id]:
                    print("  {}: {}".format(line_no, log.data[line_no]), end="")
                print()
            input()
        print()


if __name__ == "__main__":
    main()
