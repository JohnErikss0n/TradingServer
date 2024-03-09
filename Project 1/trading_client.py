import argparse
import asyncio
import os

async def send_command_to_server(reader, writer, command):
    print(f"Sending: {command}")
    writer.write(command.encode() + b'\n')
    await writer.drain()
    if command == 'report':
        # Read the header to get the content length
        header = await reader.readline()
        content_length = int(header.decode().split(": ")[1])
        received_bytes = 0

        with open("received_report.csv", "wb") as file:
            while received_bytes < content_length:
                chunk = await reader.read(min(4096, content_length - received_bytes))
                file.write(chunk)
                received_bytes += len(chunk)

        print("Report received successfully.")

    else:
        response = await reader.read(4096)
        print(f"Received: {response.decode()}")


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
