import csv

def parse_lookup_table(lookup_table_file):
    lookup_table = {}
    with open(lookup_table_file, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            print(row)
            #Storing the entries in lookup_table with (dstport, protocol) as key
            lookup_table[(row['dstport'], row['protocol'].lower())] = row['tag']
    return lookup_table


def parse_flow_logs(flow_log_file, lookup_table):
    tag_counts = {}
    port_protocol_counts = {}
    protocol_map = {'6': 'tcp', '17': 'udp', '1': 'icmp'}

    #Reading the flow log data from the file
    with open(flow_log_file, 'r') as file:
        for line in file:
            fields = line.split()
            if len(fields) < 12:
                continue  

            dstport = fields[5]
            protocol_number = fields[6]
            protocol = protocol_map.get(protocol_number, 'unknown')

            #Counting the occurrences for port/protocol combinations
            port_protocol_key = (dstport, protocol)
            if port_protocol_key in port_protocol_counts:
                port_protocol_counts[port_protocol_key] += 1
            else:
                port_protocol_counts[port_protocol_key] = 1

            tag = lookup_table.get((dstport, protocol), 'Untagged')

            #Counting the occurrences for each tag
            if tag in tag_counts:
                tag_counts[tag] += 1
            else:
                tag_counts[tag] = 1

    return tag_counts, port_protocol_counts

def write_output_file(file_path, headers, data):
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

def main():
    flow_log_file = 'flow_log.txt'           
    lookup_table_file = 'lookup_table.csv'   
    
    output_file_tag_counts = 'tag_counts.csv'
    output_file_port_protocol_counts = 'port_protocol_counts.csv'

    # Parsing the lookup table and flow logs
    lookup_table = parse_lookup_table(lookup_table_file)
    tag_counts, port_protocol_counts = parse_flow_logs(flow_log_file, lookup_table)

    # Preparing the data for writing into output files
    tag_count_data = [(tag, count) for tag, count in tag_counts.items()]
    port_protocol_data = [(port, protocol, count) for (port, protocol), count in port_protocol_counts.items()]

    # Writing the results into output files
    write_output_file(output_file_tag_counts, ['Tag', 'Count'], tag_count_data)
    write_output_file(output_file_port_protocol_counts, ['Port', 'Protocol', 'Count'], port_protocol_data)

    print(f"Tag counts have been saved to {output_file_tag_counts}")
    print(f"Port/Protocol counts have been saved to {output_file_port_protocol_counts}")

if __name__ == '__main__':
    main()
