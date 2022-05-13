/*
Utilities library for doing interval arithmetic
*/

const { List } = require("immutable")

function isWayBehind(intervalA, intervalB) {
  // A     +---+
  // B +-+  
  return intervalA.end + 1 < intervalB.start
}

function isWayAhead(intervalA, intervalB) {
  // A +-+  
  // B     +---+
  return intervalB.end + 1 < intervalA.start
}

function isBehind(intervalA, intervalB) {
  // A    +---+
  // B +-+  
  return intervalA.end < intervalB.start
}

function isAhead(intervalA, intervalB) {
  // A +-+  
  // B    +---+
  return intervalB.end < intervalA.start
}

function isJoinable(intervalA, intervalB) {
  return !(isWayBehind(intervalA, intervalB) || isWayAhead(intervalA, intervalB))
}

function isOverlap(intervalA, intervalB) {
  return !(isBehind(intervalA, intervalB) || isAhead(intervalA, intervalB))
}

function join(intervalA, intervalB) {
  if (!isJoinable(intervalA, intervalB)) {
    throw new Error("non joinable intervals")
  }

  return {
    start: Math.min(intervalA.start, intervalB.start),
    end: Math.max(intervalA.end, intervalB.end),
  }
}

function unionOne(intervals, intv) {
  if (intervals.size > 0) {
    const first = intervals.first()

    if (isWayAhead(intv, first)) {
      return unionOne(intervals.shift(), intv).unshift(first)

    } else if (isWayBehind(intv, first)) {
      return intervals.unshift(intv)

    } else {
      return unionOne(intervals.shift(), join(intv, first))

    }
  } else {
    return List.of(intv)
  }
}

function union(intervalsA, intervalsB) {
  return List(intervalsB).reduce((acc, it) => unionOne(acc, it), List(intervalsA)).toJS()
}

function differenceOne(intervals, intv) {
  if (intervals.size > 0) {
    const first = intervals.first()

    if (isAhead(intv, first)) {
      return differenceOne(intervals.shift(), intv).unshift(first)

    } else if (isBehind(intv, first)) {
      return intervals

    } else if (first.start < intv.start) {
      const leftPart = { start: first.start, end: intv.start - 1 }
      const rightPart = { start: intv.start, end: first.end }
      return differenceOne(intervals.shift().unshift(rightPart), intv).unshift(leftPart)

    } else if (intv.end < first.end) {
      // leftPart = { start: first.start, end: intv.end }
      // left part is discarded
      const rightPart = { start: intv.end + 1, end: first.end }
      return differenceOne(intervals.shift().unshift(rightPart), intv)

    } else {
      return differenceOne(intervals.shift(), intv)

    }
  } else {
    return List()
  }
}

function difference(intervalsA, intervalsB) {
  return List(intervalsB).reduce((acc, it) => differenceOne(acc, it), List(intervalsA)).toJS()
}

exports.union = union
exports.difference = difference
exports.isOverlap = isOverlap