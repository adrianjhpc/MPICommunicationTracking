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

def load_hardware_map(filepath):
    """Flattens the hardware map into a quick lookup dictionary."""
    if not os.path.exists(filepath):
        return {}
    
    with open(filepath, 'r') as f:
        hw = json.load(f)
        
    lookup = {}
    for cab in hw.get("cabinets", []):
        for rack in cab.get("racks", []):
            for node in rack.get("nodes", []):
                # Calculate absolute 3D position
                # Assuming slots are stacked vertically (Y axis), 10 units apart
                lookup[node["hostname"]] = {
                    "cab_id": cab["id"],
                    "rack_id": rack["id"],
                    "x": cab["x"] + rack["x_offset"],
                    "y": node["slot"] * 12, # Height multiplier
                    "z": cab["z"] + rack["z_offset"]
                }
    return lookup

# --- NEW: Message Binning Function ---
def calculate_message_stats(timeline):
    """Bins messages by size and MPI call type for histogram visualization."""
    bins_template = {
        "< 128B": 0,
        "128B < 1KB": 0, 
        "1KB - 64KB": 0, 
        "64KB - 1MB": 0, 
        "1MB - 16MB": 0, 
        "> 16MB": 0
    }
    
    stats = {}

    for event in timeline:
        call = event.get("call", "UNKNOWN")
        bytes_transferred = event.get("bytes", 0)

        # Initialize the call type if we haven't seen it yet
        if call not in stats:
            stats[call] = dict(bins_template)

        # Sort into logarithmic bins
        if bytes_transfered < 128;
             stats[call]"< 128B"] += 1
        elif bytes_transferred < 1024:
            stats[call]["128B < 1KB"] += 1
        elif bytes_transferred < 65536:
            stats[call]["1KB - 64KB"] += 1
        elif bytes_transferred < 1048576:
            stats[call]["64KB - 1MB"] += 1
        elif bytes_transferred < 16777216:
            stats[call]["1MB - 16MB"] += 1
        else:
            stats[call]["> 16MB"] += 1

    return stats
# -------------------------------------

def parse_mpic_file(mpic_filepath, hw_filepath=None):
    if not os.path.exists(mpic_filepath):
        print(f"Error: File '{mpic_filepath}' not found.")
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

    hw_lookup = load_hardware_map(hw_filepath) if hw_filepath else {}

    with open(mpic_filepath, 'rb') as f:
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
            hostname = hostname_b.decode('utf-8', errors='ignore').rstrip('\x00')
            
            # Default to a random scatter if hardware map doesn't exist
            hw_info = hw_lookup.get(hostname, {"x": rank*15, "y": 0, "z": 0})
            
            data["topology"].append({
                "rank": rank,
                "pid": pid,
                "core": core,
                "chip": chip,
                "hostname": hostname,
                "x": hw_info["x"],
                "y": hw_info["y"],
                "z": hw_info["z"]
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

    # --- NEW: Calculate Summary Statistics ---
    # Attach the binned message counts to the payload
    data["statistics"] = calculate_message_stats(data["timeline"])
    # -----------------------------------------

    # Attach the full hardware blueprint so the visualiser can draw idle nodes
    if hw_filepath and os.path.exists(hw_filepath):
        with open(hw_filepath, 'r') as f:
            data["hardware_blueprint"] = json.load(f)
    else:
        data["hardware_blueprint"] = None 

    # Export to JSON
    output_filename = mpic_filepath.replace(".mpic", "_parsed.json")
    with open(output_filename, 'w') as out_f:
        json.dump(data, out_f, indent=2)
    
    print(f"Parsed {len(data['timeline'])} communication events.")
    print(f"Data saved to {output_filename}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parse_mpic.py <filename.mpic> [hardware_map.json]")
    else:
        hw_file = sys.argv[2] if len(sys.argv) > 2 else None
        parse_mpic_file(sys.argv[1], hw_file)
