import argparse
import asyncio


async def clear_buffer(reader):
    """Clear any data in the buffer."""
    try:
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=1)
            if not line:  # If no more data is available, break
                break
    except asyncio.TimeoutError:
        pass  # If the timeout is reached, buffer is cleared

async def send_command_to_server(reader, writer, command):
    await clear_buffer(reader)
    print(f"Sending: {command}")
    writer.write(command.encode() + b'\n')
    await writer.drain()

    if command == 'report':
        content_length = None
        # Read the header to get the content length
        while True:
            try:
                header = await reader.readline()
                header_decoded = header.decode()
                if "Content-Length: " in header_decoded:
                    content_length = int(header_decoded.split(": ")[1])
                    break
            except Exception as e:
                print(f"Error reading header: {e}")
                break

        if content_length is None:
            print("Failed to read content length from header.")
            return

        received_bytes = 0
        with open("received_report.csv", "wb") as file:
            count = 0
            while received_bytes < content_length:
                chunk = await reader.read(min(4096, content_length - received_bytes))
                file.write(chunk)
                received_bytes += len(chunk)
                if count%10 == 0:
                    print(f"Download progress: {received_bytes / content_length * 100:.2f}%")
                count+=1
                if received_bytes >= content_length:
                    # If all bytes received, stop reading
                    break

        print("Report received successfully.")

    else:
        # For other commands
        response = await reader.read(4096)
        print(f"\nReceived: \n{response.decode('utf-8')}\n")


async def client_loop(server_ip, server_port):
    reader, writer = await asyncio.open_connection(server_ip, server_port)

    while True:
        user_input = input("Enter command: ")
        if not user_input:
            print("Exiting client.")
            break

        await send_command_to_server(reader, writer, user_input)

    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for interacting with the trading server")
    parser.add_argument('--server', type=str, required=True, help='Server address in format ip:port')
    args = parser.parse_args()

    server_ip, server_port = args.server.split(':')
    server_port = int(server_port)

    asyncio.run(client_loop(server_ip, server_port))
