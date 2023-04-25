import createRBTree, {Tree} from "functional-red-black-tree";

export interface Block {
    start: number,
    end: number,
    value: any
}

interface NodeValue {
    start: Array<{[key: number]: any}>
    end: Array<number>
}

export class Aggregator {

    // Function that takes in an item of any format and converts it into a Block for the aggregator
    private readonly itemPreProcessor: (item: any) => Block;

    // Function that allows you to define what aggregating means for your data
    private readonly aggregationFunction: (values: Array<any>) => any;

    // Function that allows you to define the output format of the aggregator
    private readonly itemPostProcessor: (block: Block) => any;

    /**
     * Creates an instance of the Aggregator.
     *
     * @param {Function} itemPreProcessor Items being processed by the aggregator need to be compatible with the Block
     * interface. A numeric start and end point is required, and any other data can be stored in the value property.
     * @param {Function} aggregationFunction Blocks that have a portion of the range between the start and end values
     * overlapping other blocks will be aggregated, but only where the overlap occurs. The aggregation function
     * determines how these overlaps are aggregated to form a new block.
     * @param {Function} itemPostProcessor This function allows you to convert the output of the aggregator into your
     * desired format, saving the need for a consecutive function.
     */
    constructor(
        itemPreProcessor: (item: any) => Block,
        aggregationFunction: (values: any[]) => any,
        itemPostProcessor: (block: Block) => any
    ) {
        this.itemPreProcessor = itemPreProcessor;
        this.aggregationFunction = aggregationFunction;
        this.itemPostProcessor = itemPostProcessor;
    }

    /**
     * Performs the aggregation of overlapping ranges with a time complexity
     * of O(nlog(n))
     *
     * @param items The items to be aggregated on range overlaps.
     */
    public aggregate(items: Array<any>): Array<any> {
        const output = new Array<any>();
        const nodeValues = new Map<number, any>();

        // The red black tree containing all the sorted data
        const redBlackTree = this.createRedBlackTree(items);

        const iterator = redBlackTree.begin;

        // Iterate over each node in the red black tree,
        // where the keys are in ascending order
        while (iterator.hasNext) {
            let start = iterator.key;

            // For each node where a block starts, add the index of that
            // block along with its value to a Map object to keep track
            // of which values need to be aggregated.
            iterator.value.start.forEach((val: {[key: number]: any}) => {
                let key = Number(Object.keys(val)[0]);

                nodeValues.set(key, val[key]);
            });

            // If a blocks range ends at the key of this node,
            // remove it from the Map as its value is no longer
            // relevant.
            iterator.value.end.forEach((val: number) => {
                nodeValues.delete(val)
            });

            iterator.next();

            // If there are no blocks with a range existing between
            // the keys of this node and the previous, no aggregation
            // needs to be performed, so it can be skipped.
            if (nodeValues.size === 0) {
                continue;
            }

            // Use the aggregation function to aggregate the
            // block values occupying this range.
            let aggregated = this.aggregationFunction(Array.from(nodeValues.values()));

            // Pass the previous key as the start value, the current key as the end value,
            // and the aggregated value into the item post processor. Then push the
            // result onto an array that will serve as the output.
            output.push(this.itemPostProcessor({
                start: <number> start,
                end: <number> iterator.key,
                value: aggregated
            }));
        }

        return output
    }

    /**
     * A red black tree is used to store the data, where the value is always within the node where the key is the
     * start value. The start and end values are effectively split across different nodes, but always have the same
     * original value and an indicator to differentiate the start from the end of a blocks range. Since a red black
     * tree is a sorted tree, you can perform the entire aggregation in a single pass.
     */
    private createRedBlackTree(items: Array<any>): Tree<number, any> {
        let redBlackTree = createRBTree<number, any>();

        for (const [i, item] of items.entries()) {

            // Converts the item to a block with a
            // numeric start and end, as well as a value
            let block = this.itemPreProcessor(item);

            // Pushes a value and a unique index for the
            // block to a node that has the key of the start value
            redBlackTree = Aggregator.pushValueToNode(
                redBlackTree, block.start, {[i]: block.value}, 'start'
            );

            // Pushes the unique index value onto the end property of a
            // node where the key is the same as the end value of the block.
            redBlackTree = Aggregator.pushValueToNode(
                redBlackTree, block.end, i, 'end'
            );
        }

        return redBlackTree;
    }

    /**
     * Initialises nodes and pushes values onto the specified property.
     *
     * @param tree The red black tree. Has to be passed in since it is a functional implementation.
     * @param key The key of the node to create or push the value onto.
     * @param value The value to push onto the property of the node.
     * @param property The property of the node to push the value onto. Either 'start' or 'end' in this case.
     * @private
     */
    private static pushValueToNode(tree: Tree<number, any>, key: number, value: any, property: string)
        : Tree<number, any> {

        let nodeValue = tree.get(key);

        if (!nodeValue) {
            nodeValue = Aggregator.emptyNodeValue();
        }

        nodeValue[property].push(value);

        return tree.remove(key).insert(key, nodeValue);
    }

    /**
     * This is the structure of each node in the tree. If the key of the node
     * (not [key: number]) matches the start value of a block, the unique index
     * of that block will be stored in an array along with the data of the block
     * in the nodes start property. Conversely, if the key is equivalent to the
     * end value of the block, only its index will be stored, this time in the
     * end property of the node.
     */
    private static emptyNodeValue(): NodeValue {
        return {
            start: new Array<{[key: number]: any}>(),
            end: new Array<number>()
        }
    }
}

