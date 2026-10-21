// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../src/ReportRegistry.sol";

contract ReportRegistryTest is Test {
    ReportRegistry reg;
    address owner;
    address stranger;

    function setUp() public {
        owner = address(this);
        stranger = makeAddr("stranger");
        reg = new ReportRegistry();
    }

    // ---- helpers ----
    function _leaf(string memory row) internal pure returns (bytes32) {
        return keccak256(bytes(row));
    }

    function _parent(bytes32 a, bytes32 b) internal pure returns (bytes32) {
        return a < b
            ? keccak256(abi.encodePacked(a, b))
            : keccak256(abi.encodePacked(b, a));
    }

    // ---- ownership ----
    function test_ownerCanPublish() public {
        bytes32 root = bytes32(uint256(1));
        vm.expectEmit(true, false, false, true);
        emit ReportRegistry.ReportPublished(20250101, root, 1, "");
        reg.publishRoot(20250101, root, 1, "");
    }

    function test_nonOwnerReverts() public {
        vm.prank(stranger);
        vm.expectRevert(Owned.NotOwner.selector);
        reg.publishRoot(20250101, bytes32(uint256(1)), 1, "");
    }

    function test_emptyRootReverts() public {
        vm.expectRevert(ReportRegistry.EmptyRoot.selector);
        reg.publishRoot(20250101, bytes32(0), 0, "");
    }

    function test_alreadyPublishedReverts() public {
        bytes32 root = bytes32(uint256(1));
        reg.publishRoot(20250101, root, 1, "");
        vm.expectRevert(abi.encodeWithSelector(ReportRegistry.AlreadyPublished.selector, uint32(20250101)));
        reg.publishRoot(20250101, bytes32(uint256(2)), 1, "");
    }

    function test_unknownDayReverts() public {
        vm.expectRevert(abi.encodeWithSelector(ReportRegistry.UnknownDay.selector, uint32(20250101)));
        reg.getReport(20250101);
    }

    function test_transferOwnership() public {
        reg.transferOwnership(stranger);
        assertEq(reg.owner(), stranger);
        // old owner loses rights
        vm.expectRevert(Owned.NotOwner.selector);
        reg.publishRoot(20250102, bytes32(uint256(1)), 1, "");
    }

    // ---- 3-leaf Merkle ----
    function test_3leaf() public {
        // rows: "a", "b", "c"
        bytes32 la = _leaf("a");
        bytes32 lb = _leaf("b");
        bytes32 lc = _leaf("c");
        bytes32 p01 = _parent(la, lb);
        // c is promoted (odd node)
        bytes32 root = _parent(p01, lc);

        reg.publishRoot(20250101, root, 3, "");

        // proof for "a": sibling lb, then sibling lc
        bytes32[] memory proofA = new bytes32[](2);
        proofA[0] = lb;
        proofA[1] = lc;
        assertTrue(reg.verifyRow(20250101, "a", proofA));

        // proof for "b": sibling la, then sibling lc
        bytes32[] memory proofB = new bytes32[](2);
        proofB[0] = la;
        proofB[1] = lc;
        assertTrue(reg.verifyRow(20250101, "b", proofB));

        // proof for "c" (promoted): sibling p01
        bytes32[] memory proofC = new bytes32[](1);
        proofC[0] = p01;
        assertTrue(reg.verifyRow(20250101, "c", proofC));

        // tampered row fails
        assertFalse(reg.verifyRow(20250101, "a_tampered", proofA));
    }

    // ---- 5-leaf Merkle ----
    function test_5leaf() public {
        bytes32[5] memory leaves;
        string[5] memory rows = ["r0", "r1", "r2", "r3", "r4"];
        for (uint i = 0; i < 5; i++) {
            leaves[i] = _leaf(rows[i]);
        }
        // level 0: [l0,l1,l2,l3,l4]
        // level 1: [p(l0,l1), p(l2,l3), l4]  (l4 promoted)
        // level 2: [p(p01,p23), l4]  (l4 promoted again)
        // root:    p(p0123, l4)
        bytes32 p01 = _parent(leaves[0], leaves[1]);
        bytes32 p23 = _parent(leaves[2], leaves[3]);
        bytes32 p0123 = _parent(p01, p23);
        bytes32 root = _parent(p0123, leaves[4]);

        reg.publishRoot(20250102, root, 5, "");

        // verify r0: proof = [l1, p23, l4]
        bytes32[] memory p = new bytes32[](3);
        p[0] = leaves[1]; p[1] = p23; p[2] = leaves[4];
        assertTrue(reg.verifyRow(20250102, "r0", p));

        // verify r4 (promoted twice): proof = [p0123]
        bytes32[] memory p4 = new bytes32[](1);
        p4[0] = p0123;
        assertTrue(reg.verifyRow(20250102, "r4", p4));
    }

    // ---- golden vector ["a","b","c"] ----
    function test_goldenVector() public {
        // Root validated against Python implementation
        bytes32 expectedRoot = 0x5842148bc6ebeb52af882a317c765fccd3ae80589b21a9b8cbf21abb630e46a7;

        bytes32 la = _leaf("a");
        bytes32 lb = _leaf("b");
        bytes32 lc = _leaf("c");
        bytes32 p01 = _parent(la, lb);
        bytes32 root = _parent(p01, lc);

        assertEq(root, expectedRoot);

        reg.publishRoot(20250103, root, 3, "");
        bytes32[] memory proofA = new bytes32[](2);
        proofA[0] = lb;
        proofA[1] = lc;
        assertTrue(reg.verifyRow(20250103, "a", proofA));
    }
}
