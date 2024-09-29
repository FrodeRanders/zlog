//
// Created by Frode Randers on 2024-09-29.
//

#include <iostream>
#include <cstring>

#if defined(__APPLE__) || defined(__MACH__)
    // macOS specific includes for kqueue
    #include <sys/types.h>
    #include <sys/event.h>
    #include <sys/time.h>
    #include <fcntl.h>
    #include <unistd.h>
#elif defined(__linux__)
    // Linux specific includes for inotify
    #include <sys/inotify.h>
    #include <unistd.h>
    #include <fcntl.h>
#endif

struct monitor_descriptor;
int monitor_file_init();
int monitor_file(const std::string& filepath);
void monitor_file_close();

#if defined(__APPLE__) || defined(__MACH__)

struct monitor_descriptor {
    int kq;
    int fd;
};

int monitor_file_init(monitor_descriptor& s) {
    s.kq = kqueue();
    if (s.kq == -1) {
        std::cerr << "Failed to create kqueue: " << strerror(errno) << std::endl;
        return 1;
    }
    return 0;
}

int monitor_file(const std::string& filepath, monitor_descriptor& s) {

    s.fd = open(filepath.c_str(), O_EVTONLY);  // Open file for event monitoring only
    if (s.fd == -1) {
        std::cerr << "Failed to open file: " << strerror(errno) << std::endl;
        close(s.kq);
        return 1;
    }

    struct kevent change;
    EV_SET(&change, s.fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, NOTE_WRITE, 0, NULL);

    std::cout << "Monitoring " << filepath << " for write on macOS." << std::endl;

    while (true) {
        struct kevent event;
        int nev = kevent(s.kq, &change, 1, &event, 1, NULL);
        if (nev == -1) {
            std::cerr << "kevent error: " << strerror(errno) << std::endl;
            break;
        }

        if (event.fflags & NOTE_WRITE) {
            std::cout << "File modified: " << filepath << std::endl;
        }
    }
    return 0;
}

void monitor_file_close(monitor_descriptor& s) {
    close(s.fd);
    close(s.kq);

    s.fd = -1;
    s.kq = -1;
}

#elif defined(__linux__)

struct monitor_descriptor {
    int inotify_fd;
    int watch_fd;
};

int monitor_file_init(monitor_descriptor& s) {
    s.inotify_fd = inotify_init();
    if (s.inotify_fd == -1) {
        std::cerr << "Failed to initialize inotify: " << strerror(errno) << std::endl;
        return 1;
    }
    return 0;
}

int monitor_file(const std::string& filepath, monitor_descriptor& s) {
    s.watch_fd = inotify_add_watch(s.inotify_fd, filepath.c_str(), IN_MODIFY | IN_CLOSE_WRITE | IN_DELETE);
    if (s.watch_fd == -1) {
        std::cerr << "Failed to add watch: " << strerror(errno) << std::endl;
        close(s.inotify_fd);
        s.inotify_fd = -1;
        return 1;
    }

    std::cout << "Monitoring " << filepath << " for changes on Linux." << std::endl;

    const size_t buf_size = 1024 * sizeof(struct inotify_event);
    char buffer[buf_size];

    while (true) {
        ssize_t length = read(s.inotify_fd, buffer, buf_size);
        if (length < 0) {
            std::cerr << "Error reading inotify events: " << strerror(errno) << std::endl;
            break;
        }

        size_t i = 0;
        while (i < length) {
            struct inotify_event* event = (struct inotify_event*)&buffer[i];

            if (event->mask & IN_CLOSE_WRITE) {
                std::cout << "File closed after writing: " << filepath << std::endl;
                break;
            }

            i += sizeof(struct inotify_event) + event->len;
        }
    }
    return 0;
}

void monitor_file_close(monitor_descriptor& s) {
    close(s.watch_fd);
    close(s.inotify_fd);

    s.watch_fd = -1;
    s.inotify_fd = -1;
}

#else
#error "This platform is not supported."
#endif

