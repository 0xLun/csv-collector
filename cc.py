import csv
import json
import argparse
import os
import re
import sys


def log(message, level="info", verbosity=0, min_level=1):
    levels = {"error": "[ERROR]", "warn": "[WARNING]", "info": "[INFO]", "debug": "[DEBUG]"}
    if verbosity >= min_level:
        print(f"{levels[level]} {message}", file=sys.stderr if level == "error" else sys.stdout)


def load_config(config_path, verbosity):
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            log(f"Loading config file: {config_path}", "debug", verbosity, 2)
            return json.load(f)
    except Exception as e:
        log(f"Unable to load config file '{config_path}': {e}", "error")
        sys.exit(1)


def compute_output_fields(config, sample_row):
    base_fields = list(sample_row.keys())
    added_fields = []

    for field in config.get("add-fields", []):
        name = field["name"]
        after = field.get("after")
        default = field.get("default-value")

        if name in base_fields:
            continue

        insert_index = len(base_fields)
        if after and after in base_fields:
            insert_index = base_fields.index(after) + 1

        base_fields.insert(insert_index, name)
        added_fields.append((name, default))

    for name, default in added_fields:
        sample_row[name] = default

    output_fields = config.get("output", {}).get("fields")
    if not output_fields:
        output_fields = list(sample_row.keys())

    rule_field = config["output"].get("rule-match-field", "_rule")
    file_field = config["output"].get("file-processed-field", "_file")

    if rule_field not in output_fields:
        output_fields.insert(0, rule_field)
    if file_field not in output_fields:
        output_fields.insert(1, file_field)

    config["output"]["computed_fields"] = output_fields
    return output_fields


def apply_match_assertions(row, rule, regex_flags):
    match_summary = []
    assertions =  rule.get("match", [])
    for assertion in assertions:
        for field in assertion.get("fields", []):
            if field in row:
                value = row[field]
                match_obj = re.search(assertion["regex"], value, flags=regex_flags)
                if match_obj:
                    match_summary.append(f"{field}:{match_obj.group(0)}")
                    break
                    
    return len(match_summary)>=len(assertions), match_summary


def apply_write_truth(row, rule, match_summary):
    write_truth = rule.get("write-truth")
    if write_truth:
        target_field = write_truth.get("field")
        value = write_truth.get("value", "")
        if "$match" in value:
            summary_str = " AND ".join(match_summary)
            value = value.replace("$match", summary_str)
        row[target_field] = value


def process_row(config, row, file_path, row_number, verbosity):
    rule_field = config["output"].get("rule-match-field", "_rule")
    processed_rows = []
    any_rule_matched = False

    for rule in config.get("rules", []):
        regex_flags = 0 if rule.get("case-sensitive", False) else re.IGNORECASE
        rule_name = rule.get("name", "no-name")
        action = rule.get("action")
        write_truth = rule.get("write-truth")

        # Evaluate match assertions and gather a summary.
        matched, match_summary = apply_match_assertions(row, rule, regex_flags)

        if not matched:
            continue
        
        log(f"Match rule {rule['name']} in {file_path} at row {row_number} ", "info", verbosity, 2)

        any_rule_matched = True

        # If a drop rule matches, drop the row immediately.
        if action == "drop-row":
            log(f"Dropping row {row_number} due to rule '{rule_name}'", "info", verbosity, 2)
            return []  # Do not output anything for this row.

        # For a replace action, update the row in place.
        if action == "replace":
            for assertion in rule.get("match", []):
                for field in assertion.get("fields", []):
                    regex = assertion.get("regex")
                    if field in row:
                        row[field] = re.sub(regex, rule.get("replace-by", ""), row[field], flags=regex_flags)
            # Apply write-truth if configured.
            apply_write_truth(row, rule, match_summary)

        # For a keep-row action, create a copy of the row with modifications.
        if action == "keep-row":
            row_copy = row.copy()
            apply_write_truth(row_copy, rule, match_summary)
            row_copy[rule_field] = rule_name
            processed_rows.append(row_copy)

    # If no rule matched (and no drop occurred), return the original row.
    if not any_rule_matched :
        if "drop-unmatched" in config["output"] or config["output"]["drop-unmatched"] :
            return [] 
        return [row]

    # If one or more rules have matched (for replace, the row was modified in place),
    # and no keep-row rule was applied, output the modified row.
    if not processed_rows:
        return [row]

    return processed_rows


def process_csv(file_path, config, writer, verbosity):
    rule_field = config["output"].get("rule-match-field", "_rule")
    file_field = config["output"].get("file-processed-field", "_file")

    try:
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            if not reader.fieldnames:
                log(f"Skipping file '{file_path}' - no headers found.", "warn", verbosity, 1)
                return

            log(f"Processing file: {file_path}", "info", verbosity, 1)

            for row_number, row in enumerate(reader, start=1):
                if not row:
                    log(f"Skipping empty row {row_number} in '{file_path}'", "warn", verbosity, 3)
                    continue

                if "computed_fields" not in config["output"]:
                    computed = compute_output_fields(config, row.copy())
                    writer.fieldnames = computed
                    writer.writeheader()

                log(f"Processing row {row_number} in '{file_path}'", "debug", verbosity, 3)
                matched_rows = process_row(config, row, file_path, row_number, verbosity)

                for matched_row in matched_rows:
                    matched_row[file_field] = os.path.basename(file_path)
                    output_row = {key: matched_row.get(key, "") for key in writer.fieldnames}
                    writer.writerow(output_row)

    except Exception as e:
        log(f"Unable to process file '{file_path}': {e}", "error")


def process_directory(input_path, config, writer, verbosity):
    if not os.path.exists(input_path):
        log(f"The input directory '{input_path}' does not exist.", "error")
        sys.exit(1)

    csv_files = [f for f in os.listdir(input_path) if f.endswith(".csv")]
    if not csv_files:
        log(f"No CSV files found in directory '{input_path}'.", "warn", verbosity, 1)
        return

    for file_name in csv_files:
        file_path = os.path.join(input_path, file_name)
        process_csv(file_path, config, writer, verbosity)


def main():
    parser = argparse.ArgumentParser(description="Parse CSV files based on regex rules")
    parser.add_argument("-i", "--input", required=True, help="Input CSV file or directory")
    parser.add_argument("-o", "--output", required=True, help="Output CSV file")
    parser.add_argument("-c", "--config", required=True, help="Path to config.json file")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity level (-v for info, -vv for debug)")

    args = parser.parse_args()
    config = load_config(args.config, args.verbose)

    try:
        with open(args.output, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[])  # fieldnames updated in process_csv
            if os.path.isdir(args.input):
                process_directory(args.input, config, writer, args.verbose)
            else:
                process_csv(args.input, config, writer, args.verbose)

    except Exception as e:
        log(f"Unable to write to output file '{args.output}': {e}", "error")
        sys.exit(1)


if __name__ == "__main__":
    main()
