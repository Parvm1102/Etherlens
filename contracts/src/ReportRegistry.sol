// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @notice Minimal ownership base contract. ReportRegistry inherits from it
///         (demonstrates Solidity inheritance, notes §3.16).
abstract contract Owned {
    address public owner;

    error NotOwner();
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);

    constructor() {
        owner = msg.sender;
        emit OwnershipTransferred(address(0), msg.sender);
    }

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }

    function transferOwnership(address newOwner) external onlyOwner {
        require(newOwner != address(0), "zero owner");
        emit OwnershipTransferred(owner, newOwner);
        owner = newOwner;
    }
}

/// @title ReportRegistry
/// @notice Append-only registry of Merkle roots of daily analytics reports.
///         Leaves are keccak256(bytes(row)); parents hash the sorted pair.
contract ReportRegistry is Owned {
    struct Report {
        bytes32 root;
        uint32 leafCount;
        uint64 publishedAt;
        string uri;
    }

    mapping(uint32 => Report) private _reports; // day as yyyymmdd => report
    uint32[] private _days;

    event ReportPublished(uint32 indexed day, bytes32 root, uint32 leafCount, string uri);

    error EmptyRoot();
    error AlreadyPublished(uint32 day);
    error UnknownDay(uint32 day);

    function publishRoot(uint32 day, bytes32 root, uint32 leafCount, string calldata uri)
        external
        onlyOwner
    {
        if (root == bytes32(0)) revert EmptyRoot();
        if (_reports[day].root != bytes32(0)) revert AlreadyPublished(day);
        _reports[day] = Report(root, leafCount, uint64(block.timestamp), uri);
        _days.push(day);
        emit ReportPublished(day, root, leafCount, uri);
    }

    function isPublished(uint32 day) external view returns (bool) {
        return _reports[day].root != bytes32(0);
    }

    function getReport(uint32 day) external view returns (Report memory) {
        Report memory r = _reports[day];
        if (r.root == bytes32(0)) revert UnknownDay(day);
        return r;
    }

    function publishedDays() external view returns (uint32[] memory) {
        return _days;
    }

    /// @notice Check that `row` (exact text line of the day's report) is part of the published report.
    function verifyRow(uint32 day, string calldata row, bytes32[] calldata proof)
        external
        view
        returns (bool)
    {
        return _verify(day, keccak256(bytes(row)), proof);
    }

    function verifyLeaf(uint32 day, bytes32 leaf, bytes32[] calldata proof)
        external
        view
        returns (bool)
    {
        return _verify(day, leaf, proof);
    }

    function _verify(uint32 day, bytes32 leaf, bytes32[] calldata proof)
        internal
        view
        returns (bool)
    {
        bytes32 root = _reports[day].root;
        if (root == bytes32(0)) revert UnknownDay(day);
        bytes32 h = leaf;
        for (uint256 i = 0; i < proof.length; ++i) {
            bytes32 p = proof[i];
            h = h < p ? keccak256(abi.encodePacked(h, p)) : keccak256(abi.encodePacked(p, h));
        }
        return h == root;
    }
}
