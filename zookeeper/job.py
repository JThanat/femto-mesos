class Jobstate(object):
    PENDING = 1
    STAGING = 2
    RUNNING = 3
    SUCCESSFUL = 4
    FAILED = 5

    state = [
        "PENDING",
        "STAGING",
        "RUNNING",
        "SUCCESSFUL",
        "FAILED"
    ]

    @classmethod
    def getstate_name(self, state):
        if state < 1 or state > 5:
            raise ValueError("The state is not defined")

        return self.state[state-1]