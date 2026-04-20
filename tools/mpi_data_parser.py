import struct
import json
import sys
import os

# --- MPI Message Type Mapping ---
# Matches the definitions in mpi_communication_tracking.h
MESSAGE_TYPES = {
    13: "MPI_SEND",      14: "MPI_RECV",      15: "MPI_BSEND",
    16: "MPI_SSEND",     17: "MPI_RSEND",     18: "MPI_ISEND",
    19: "MPI_IBSEND",    20: "MPI_ISSEND",    21: "MPI_IRSEND",
    22: "MPI_IRECV",     23: "MPI_SENDRECV",  24: "MPI_WAIT",
    25: "MPI_WAITALL",   26: "MPI_BARRIER",   27: "MPI_BCAST",
    28: "MPI_REDUCE",    29: "MPI_ALLREDUCE", 30: "MPI_GATHER",
    31: "MPI_SCATTER",   32: "MPI_ALLGATHER"
}

def parse_mpic_file(filepath):
    if not os.path.exists(filepath):
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)

    # Define struct formats based on standard C sizes (i=4 bytes, d=8 bytes, s=char)
    # Using '=' to enforce standard size without native padding (which usually aligns with C arrays)
    process_info_fmt = '=i i i i 1024s'
    p2p_small_fmt = '=d i i i i i i'
    p2p_large_fmt = '=d i i i i i i i i i i'

    process_info_size = struct.calcsize(process_info_fmt)
    small_size = struct.calcsize(p2p_small_fmt)
    large_size = struct.calcsize(p2p_large_fmt)

    data = {
        "metadata": {"total_ranks": 0},
        "topology": [],
        "timeline": []
    }

    with open(filepath, 'rb') as f:
        # Read Total Processes (my_size)
        my_size_bytes = f.read(4)
        if not my_size_bytes:
            print("Error: Empty file.")
            sys.exit(1)
        
        data["metadata"]["total_ranks"] = struct.unpack('=i', my_size_bytes)[0]

        # Read Process Information (Nodes)
        for _ in range(data["metadata"]["total_ranks"]):
            proc_bytes = f.read(process_info_size)
            rank, pid, core, chip, hostname_b = struct.unpack(process_info_fmt, proc_bytes)
            
            data["topology"].append({
                "rank": rank,
                "pid": pid,
                "core": core,
                "chip": chip,
                # Decode bytes to string and strip null characters
                "hostname": hostname_b.decode('utf-8', errors='ignore').rstrip('\x00')
            })

        # Read Communication Data per Rank
        for _ in range(data["metadata"]["total_ranks"]):
            # Read Rank ID
            rank_id = struct.unpack('=i', f.read(4))[0]

            # Read Small Messages Header
            small_header = f.read(24).decode('utf-8', errors='ignore').rstrip('\x00')
            num_small = struct.unpack('=i', f.read(4))[0]

            # Read Small Messages
            for _ in range(num_small):
                sm_bytes = f.read(small_size)
                time_val, msg_id, mtype, sender, receiver, count, bytes_vol = struct.unpack(p2p_small_fmt, sm_bytes)
                
                data["timeline"].append({
                    "time": time_val,
                    "event_id": msg_id,
                    "rank_recording": rank_id,
                    "call": MESSAGE_TYPES.get(mtype, f"UNKNOWN_{mtype}"),
                    "sender": sender,
                    "receiver": receiver,
                    "count": count,
                    "bytes": bytes_vol,
                    "category": "point-to-point"
                })

            # Read Large Messages Header
            large_header = f.read(24).decode('utf-8', errors='ignore').rstrip('\x00')
            num_large = struct.unpack('=i', f.read(4))[0]

            # Read Large Messages
            for _ in range(num_large):
                lg_bytes = f.read(large_size)
                (time_val, msg_id, mtype, 
                 s1, r1, c1, b1, 
                 s2, r2, c2, b2) = struct.unpack(p2p_large_fmt, lg_bytes)

                call_name = MESSAGE_TYPES.get(mtype, f"UNKNOWN_{mtype}")
                
                # Split large data structs (like Sendrecv or Gather) into two distinct timeline events
                # so the visualizer can draw both halves of the data movement independently
                data["timeline"].extend([
                    {
                        "time": time_val,
                        "event_id": msg_id,
                        "rank_recording": rank_id,
                        "call": call_name,
                        "sender": s1,
                        "receiver": r1,
                        "count": c1,
                        "bytes": b1,
                        "category": "collective_part_1"
                    },
                    {
                        "time": time_val,
                        "event_id": msg_id,
                        "rank_recording": rank_id,
                        "call": call_name,
                        "sender": s2,
                        "receiver": r2,
                        "count": c2,
                        "bytes": b2,
                        "category": "collective_part_2"
                    }
                ])

    # Sort all events chronologically so the visualizer reads them in exact order
    data["timeline"].sort(key=lambda x: x["time"])

    # Export to JSON
    output_filename = filepath.replace(".mpic", "_parsed.json")
    with open(output_filename, 'w') as out_f:
        json.dump(data, out_f, indent=2)
    
    print(f"Success! Parsed {len(data['timeline'])} communication events.")
    print(f"Data saved to {output_filename}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parse_mpic.py <filename.mpic>")
    else:
        parse_mpic_file(sys.argv[1])
