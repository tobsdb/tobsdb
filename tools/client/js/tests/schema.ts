import { PrimaryKey, Unique, Default } from "../src/index";

export type Schema = {
  example: {
    id: PrimaryKey<number>;
    name: Default<string>;
    vector: number[];
    createdAt: Default<Date>;
  };
  first: {
    id: PrimaryKey<number>;
    createdAt: Default<Date>;
    updatedAt?: Date;
    user: number;
  };
  second: {
    id: PrimaryKey<number>;
    createdAt: Default<Date>;
    updatedAt?: Date;
    rel_str: string;
  };
  third: {
    id: PrimaryKey<number>;
    str: Unique<string>;
  };
  nested_vec: {
    id: PrimaryKey<number>;
    vec2: number[][];
    vec3?: string[][][];
  };
};
