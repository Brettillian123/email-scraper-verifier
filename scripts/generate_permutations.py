import argparse

from src.generate.permutations import generate_permutations, infer_domain_pattern


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--first", required=True)
    p.add_argument("--last", required=True)
    p.add_argument("--domain", required=True)
    p.add_argument("--published", nargs="*", default=[])
    args = p.parse_args()

    only = infer_domain_pattern(args.published, args.first, args.last) if args.published else None
    emails = sorted(generate_permutations(args.first, args.last, args.domain, only_pattern=only))
    for e in emails:
        print(e)


if __name__ == "__main__":
    main()
