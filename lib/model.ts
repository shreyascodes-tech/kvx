import { z } from "https://deno.land/x/zod@v3.17.3/mod.ts";

export type WithId<T> = T & { id: string };

class Model<
  Schema extends z.AnyZodObject,
  T extends z.infer<Schema>,
  Indexes extends (keyof T)[] = []
> {
  constructor(
    private key: string,
    private schema: Schema,
    private indexes: Indexes
  ) {}

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

  find(kv: Deno.Kv, id: string, consistency?: Deno.KvConsistencyLevel) {
    const key = [this.key, id];
    return kv.get<WithId<T>>(key, { consistency });
  }

  findAll(kv: Deno.Kv, options?: Deno.KvListOptions) {
    return kv.list<WithId<T>>({ prefix: [this.key] }, options);
  }

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

export function model<
  T extends z.AnyZodObject,
  Indexes extends (keyof z.infer<T>)[]
>(key: string, schema: T, indexes: Indexes) {
  return new Model<T, z.infer<T>, Indexes>(key, schema, indexes);
}
