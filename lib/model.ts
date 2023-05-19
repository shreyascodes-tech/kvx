import { z } from "https://deno.land/x/zod@v3.17.3/mod.ts";

export type WithId<T> = T & { id: string };

/**
 * A model is a collection of data that can be stored in a KV store.
 * It is defined by a schema, a key, and a set of indexes.
 *
 * The schema is a Zod schema that defines the shape of the data.
 * The key is a string that is used as a prefix for all keys in the KV store.
 * The indexes are a list of keys in the schema that should be indexed.
 *
 * The model can be used to create, read, update, and delete data in the KV store.
 * It can also be used to find data by index.
 *
 * ```ts
 * const userSchema = z.object({
 *  name: z.string(),
 *  email: z.string().email(),
 * });
 *
 * const user = model("user", userSchema, ["email"]);
 *
 * await user.create(kv, {
 *  name: "John Doe",
 *  email: "test@test.com",
 * });
 *
 * const user = await user.findByIndex(kv, "email", "test@test.com");
 * ```
 */
class Model<
  Schema extends z.AnyZodObject,
  T extends z.infer<Schema>,
  Indexes extends (keyof T)[] = []
> {
  constructor(
    private key: string,
    private schema: Schema,
    private indexes: Indexes = [] as unknown as Indexes
  ) {}

  /**
   * Create a new record in the KV store.
   *
   * ```ts
   * const user = model("user", z.object({
   *   name: z.string(),
   *   email: z.string().email(),
   * }), ["email"]);
   *
   * await user.create(kv, {
   *    name: "John Doe",
   *    email: "test@test.com",
   * });
   * ```
   */
  create(kv: Deno.Kv, value: T) {
    const id = crypto.randomUUID();
    const key = [this.key, id];

    const data = {
      ...value,
      id,
    };

    let op = kv.atomic().set(key, data);

    for (let i = 0; i < this.indexes.length; i++) {
      const index = this.indexes[i] as string;
      const indexKey = [`${this.key}_by_${index}`, value[index]];

      op = op.set(indexKey, id);
    }

    return op.commit();
  }

  /**
   * Find a record in the KV store by its ID.
   * ```ts
   * const user = model("user", z.object({
   *  name: z.string(),
   *  email: z.string().email(),
   * }));
   *
   * const user = await user.find(kv, "123");
   */
  find(kv: Deno.Kv, id: string, consistency?: Deno.KvConsistencyLevel) {
    const key = [this.key, id];
    return kv.get<WithId<T>>(key, { consistency });
  }

  /**
   * Find all records in the KV store.
   * ```ts
   * const user = model("user", z.object({
   *  name: z.string(),
   *  email: z.string().email(),
   * }));
   *
   * for await (const { key, value } of user.findAll(kv)) {
   *  console.log(key, value);
   * }
   * ```
   */
  findAll(kv: Deno.Kv, options?: Deno.KvListOptions) {
    return kv.list<WithId<T>>({ prefix: [this.key] }, options);
  }

  /**
   * Find a record in the KV store by an indexed value.
   *
   * ```ts
   * const user = model("user", z.object({
   *    name: z.string(),
   *    email: z.string().email(),
   * }), ["email"]);
   *
   * const user = await user.findByIndex(kv, "email", "test@test.com");
   *
   * ```
   */
  async findByIndex(
    kv: Deno.Kv,
    index: Indexes[number],
    value: T[Indexes[number]],
    consistency?: Deno.KvConsistencyLevel
  ) {
    const indexKey = [`${this.key}_by_${index as string}`, value];
    const id = await kv.get<string>(indexKey, { consistency });
    if (!id || !id.value) {
      return;
    }

    return kv.get<WithId<T>>([this.key, id.value], { consistency });
  }

  /**
   * Update a record in the KV store.
   * ```ts
   * const user = model("user", z.object({
   *    name: z.string(),
   *    email: z.string().email(),
   * }));
   *
   * await user.update(kv, "123", {
   *    name: "John Doe",
   * });
   *
   * ```
   */
  update(kv: Deno.Kv, id: string, value: Partial<T>) {
    const key = [this.key, id];

    const data = kv.get(key);
    if (!data) {
      return;
    }

    const newData = {
      ...data,
      ...value,
    };

    return kv.set(key, newData);
  }

  /**
   * Delete a record in the KV store.
   * ```ts
   * const user = model("user", z.object({
   *   name: z.string(),
   *   email: z.string().email(),
   * }));
   *
   * await user.delete(kv, "123");
   * ```
   */
  async delete(kv: Deno.Kv, id: string) {
    const key = [this.key, id];
    const data = await kv.get<WithId<T>>(key);
    if (!data || !data.value) {
      return;
    }

    let op = kv.atomic().delete(key);
    for (let i = 0; i < this.indexes.length; i++) {
      const index = this.indexes[i] as string;
      const indexKey = [`${this.key}_by_${index}`, data?.value[index]];

      op = op.delete(indexKey);
    }
    return op.commit();
  }
}

/**
 * A model is a collection of data that can be stored in a KV store.
 * It is defined by a schema, a key, and a set of indexes.
 *
 * The schema is a Zod schema that defines the shape of the data.
 * The key is a string that is used as a prefix for all keys in the KV store.
 * The indexes are a list of keys in the schema that should be indexed.
 *
 * The model can be used to create, read, update, and delete data in the KV store.
 * It can also be used to find data by index.
 *
 * ```ts
 * const userSchema = z.object({
 *  name: z.string(),
 *  email: z.string().email(),
 * });
 *
 * const user = model("user", userSchema, ["email"]);
 *
 * await user.create(kv, {
 *  name: "John Doe",
 *  email: "test@test.com",
 * });
 *
 * const user = await user.findByIndex(kv, "email", "test@test.com");
 * ```
 */
export function model<
  T extends z.AnyZodObject,
  Indexes extends (keyof z.infer<T>)[]
>(key: string, schema: T, indexes?: Indexes) {
  return new Model<T, z.infer<T>, Indexes>(key, schema, indexes);
}
