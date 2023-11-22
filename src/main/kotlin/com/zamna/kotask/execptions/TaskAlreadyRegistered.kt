package com.zamna.kotask.execptions

class TaskAlreadyRegistered(taskName: String) : Throwable() {
    override val message: String = "Task $taskName is already registered."
}
