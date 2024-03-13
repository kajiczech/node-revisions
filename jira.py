import time


class Revision:
    """Revision's hash consists of the hash of the input and the hashes of the child revisions.
    It also keeps the output of the calculation for the given input and child revisions.
    The hash can be calculated immediatelly only based on the input
    """

    node: "Node"
    hash: str
    input_hash: str
    child_revisions: dict["Node", "Revision"]
    output: str | None = ""

    def __init__(self, node: "Node", child_revisions: dict["Node", "Revision"]):
        self.node = node
        self.input = node.data
        self.input_hash = node.current_input_hash
        self.child_revisions = child_revisions
        self.hash = "".join(revision.hash for revision in child_revisions.values()) + self.input_hash

    def calculate_output(self) -> str:
        output = self.input
        for revision in self.child_revisions.values():
            if not revision.output:
                raise ValueError("Child revision has no output")
            output += revision.output
        self.output = self.process_output(output)

        for master in self.node.masters:
            master.refresh()  # can be async Task

        return self.output

    def process_output(self, output: str) -> str:
        time.sleep(30)
        return output


class Node:
    revisions: list[Revision] = []
    masters: list["Node"] = []  # can have more than one parent/master - circular dependency needs to be checked
    slaves: list["Node"] = []
    data = ""

    def lock(self) -> None:
        pass

    def unlock(self) -> None:
        pass

    @property
    def current_input_hash(self) -> str:
        return str(self.data)

    @property
    def current_revision(self) -> Revision:
        return self.revisions[-1] if self.revisions else None

    def refresh(self) -> Revision:
        """First, figure out if something changed. If so, create a new revision. Then, refresh all slaves. If all slaves
        have complete output, calculate the output of the current revision of the node."""

        self.lock()
        new_child_revisions = {}
        has_complete_output = True
        changed = not self.revisions or self.current_input_hash != self.current_revision.input_hash

        for slave in self.slaves:
            """Check if any slave has a new revision"""
            revision = slave.refresh()
            new_child_revisions[slave] = revision

            if not revision.output:
                has_complete_output = False

            if self.current_revision.child_revisions[slave] != slave.current_revision:
                changed = True

        if changed:
            self.revisions.append(Revision(self, child_revisions=new_child_revisions))

        if has_complete_output:
            self.schedule_calculate_output()  # This would be async Task

        self.unlock()
        return self.current_revision

    def schedule_calculate_output(self):
        self.current_revision.calculate_output()





