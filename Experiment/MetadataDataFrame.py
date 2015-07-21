from pandas import DataFrame


class MetadataDataFrame(DataFrame):
    def __init__(
            self,
            data=None,
            index=None,
            columns=None,
            meta_info={},
            name=None,
            dtype=None,
            copy=False
    ):
        super(MetadataDataFrame, self).__init__(
            data=data,
            index=index,
            columns=columns,
            dtype=dtype,
            copy=copy,
            )
        self.meta_info = meta_info
        self.name = name

    def __reduce__(self):
        return self.__class__, (
            DataFrame(self),  # NOTE Using that type(data)==DataFrame and the
            # the rest of the arguments of DataFrame.__init__
            # to defaults, the constructors acts as a
            # copy constructor.
            None,
            None,
            self.meta_info,
            self.name,
            None,
            False,
        )

    def append(self, other, ignore_index=False, verify_integrity=False):
        result = super(MetadataDataFrame, self).append(other, ignore_index, verify_integrity)
        result.meta_info = self.meta_info
        return result