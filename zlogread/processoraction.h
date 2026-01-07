//
// Created by Frode Randers on 2024-10-12.
//

#ifndef PROCESSOR_ACTION_H
#define PROCESSOR_ACTION_H

#include <string>

class ProcessorAction {
public:
    virtual ~ProcessorAction() = default;
    virtual void flush(const std::string& reason) = 0;
};

class ObjectStoreAction final : public ProcessorAction {
public:
    void flush(const std::string& reason) override;
};

#endif // PROCESSOR_ACTION_H
