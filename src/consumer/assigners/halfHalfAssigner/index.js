const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')

/**
 * HalfHalfAssigner
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => ({
  name: 'HalfHalfAssigner',
  version: 0,

  /**
   * Assign the topics to the provided members.
   *
   * The members array contains information about each member, `memberMetadata` is the result of the
   * `protocol` operation.
   *
   * @param {object} group
   * @param {import('types').GroupMember[]} group.members array of members, e.g:
                              [{ memberId: 'test-5f93f5a3', memberMetadata: Buffer }]
   * @param {string[]} group.topics
   * @returns {Promise<import('types').GroupMemberAssignment[]>} object partitions per topic per member, e.g:
   *                   [
   *                     {
   *                       memberId: 'test-5f93f5a3',
   *                       memberAssignment: {
   *                         'topic-A': [0, 2, 4, 6],
   *                         'topic-B': [1],
   *                       },
   *                     },
   *                     {
   *                       memberId: 'test-3d3d5341',
   *                       memberAssignment: {
   *                         'topic-A': [1, 3, 5],
   *                         'topic-B': [0, 2],
   *                       },
   *                     }
   *                   ]
   */
  async assign({ members, topics }) {
    const membersCount = members.length
    const sortedMembers = members.map(({ memberId }) => memberId).sort()
    const assignment = {}

    const topicsPartitions = topics.flatMap(topic => {
      const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
      return partitionMetadata.map(m => ({ topic: topic, partitionId: m.partitionId }))
    })

    // count the number of partitions
    const numPartitions = topicsPartitions.length
    // partitions per member
    const partitionsPerMember = Math.ceil(numPartitions / membersCount)

    for (let i = 0; i < membersCount; i++) {
        assignment[sortedMembers[i]] = Object.create(null)

        // assign half (ceiling) of the partitions
        for (let j = 0; j < partitionsPerMember; j++) {
          if (i * partitionsPerMember + j >= numPartitions) {
            break
          }
            const topicPartition = topicsPartitions[i * partitionsPerMember + j]
            if (!assignment[sortedMembers[i]][topicPartition.topic]) {
                assignment[sortedMembers[i]][topicPartition.topic] = []
            }
            assignment[sortedMembers[i]][topicPartition.topic].push(topicPartition.partitionId)
        }
    }

    return Object.keys(assignment).map(memberId => ({
      memberId,
      memberAssignment: MemberAssignment.encode({
        version: this.version,
        assignment: assignment[memberId],
      }),
    }))
  },

  protocol({ topics }) {
    return {
      name: this.name,
      metadata: MemberMetadata.encode({
        version: this.version,
        topics,
      }),
    }
  },
})
