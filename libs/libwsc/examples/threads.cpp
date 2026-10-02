#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "WebSocketClient.h" // Include the thread-safe client class

int main() {
    WebSocketClient client;
    
    client.setUrl("ws://192.168.0.24:3001");

    client.setMessageCallback([](const std::string& message) {
        std::cout << "Received: " << message << std::endl;
    });

    int count= 0;
    client.setBinaryCallback([&count](const void* data, size_t length) {
        count++;
        const uint8_t* bytes = static_cast<const uint8_t*>(data);
        std::cout << "Received binary data (" << length << " bytes): " << " Count: " << count;
        
        // Print first 16 bytes as hex
        size_t display_bytes = std::min(length, size_t(16));
        for (size_t i = 0; i < display_bytes; i++) {
            std::cout << std::hex << static_cast<int>(bytes[i]) << " ";
        }
        std::cout << std::dec << std::endl;
    });

    client.setErrorCallback([&client](int error_code, const std::string& error_message) {
        std::cout << "Error Callback in Main App: Calling disconnect" << std::endl;
        client.disconnect();
    });
    
    // Connect to server
    client.connect();
    
    // Send a message from main thread
    client.sendMessage("Hello from main thread!");
    
    // Create another thread that sends messages - to demonstrate thread safety
    std::thread message_thread([&client]() {
        for (int i = 0; i < 5; i++) {
            std::string msg = "Message from second thread: " + std::to_string(i);
            client.sendMessage(msg);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            if(i==4) {
                std::vector<uint8_t> binary_data = {
                    0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC
                };
                client.sendBinary(binary_data.data(), binary_data.size());
                client.sendMessage("This is the final message from thread");
            }
        }
    });

    // Send binary data explicitly
    std::vector<uint8_t> binary = {0x01, 0x02, 0x03};
    client.sendBinary(binary.data(), binary.size());
    
    // Wait for the message thread to complete
    message_thread.join();
    

    client.sendMessage("This is the final message from main thread!");

    // Keep the program running to receive responses
    std::cout << "Press Enter to quit..." << std::endl;
    std::cin.get();
    
    // Disconnect when done
    client.disconnect();
    
    return 0;
}